"""User-invoked sync client. Read-only source; owner-only writes; no scheduler."""
from __future__ import annotations

import contextlib
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import sys
import sqlite3
import urllib.error
import urllib.request

from .model import SourceReader, SyncConverter, clean_messages, codec, fingerprint, legacy_seed, native_groups


def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.new')
    with temp.open('w') as f:
        json.dump(value,f,ensure_ascii=False,sort_keys=True);f.write('\n');f.flush();os.fsync(f.fileno())
    temp.chmod(0o600);os.replace(temp,path)


def native_call(home,endpoint,path,value=None):
    from hermes_cli.gateway_client import _session_ticket
    ticket=_session_ticket(home,endpoint,purpose='native-http')
    data=json.dumps(value,ensure_ascii=False,allow_nan=False).encode() if value is not None else None
    request=urllib.request.Request(endpoint.api_origin+'/api/plugins/opencode-sync/'+path,data=data,
        method='POST' if data is not None else 'GET',headers={'x-hermes-gateway-ticket':ticket,'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(request,timeout=180) as response:return json.load(response)
    except urllib.error.HTTPError as exc:
        try:detail=json.load(exc).get('detail',str(exc))
        except ValueError:detail=str(exc)
        raise ValueError(str(detail)) from exc


def initial_state(home,db,source,cached):
    seeds,assets,snapshot_paths=legacy_seed(home)
    if db is None:
        seeds={};cached={};snapshot_paths={}
    entries=cached.get('entries') or seeds
    groups=native_groups(db)
    for sid,rows in groups.items():
        if sid not in entries:
            ordered=sorted(rows,key=lambda item:int(item[1].get('segment') or 1))
            entries[sid]={'ids':[r[0] for r in ordered],
                          'baselines':{r[0]:r[2].get('message_fingerprint') for r in ordered},'signature':None}
        entry=entries[sid]
        signatures=set()
        for hid,origin,sync in rows:
            if sync.get('message_fingerprint'):
                entry.setdefault('baselines',{})[hid]=sync['message_fingerprint']
                signatures.add(sync.get('source_signature'))
                if hid not in entry['ids']:entry['ids'].append(hid)
        complete = (set(entry['ids']) == {r[0] for r in rows}
                    and all(r[2].get('message_fingerprint') and r[2].get('source_signature') for r in rows))
        if entry.get('signature') is None and complete and len(signatures)==1 and None not in signatures:
            entry['signature']=next(iter(signatures))
    return {'version':1,'source_database':str(source.path),'entries':entries},assets,snapshot_paths,groups


def main(args):
    from hermes_constants import get_hermes_home
    from hermes_state import SessionDB
    from hermes_cli.gateway_runtime import discover_gateway_endpoint,ensure_gateway_runtime,attachable
    home=get_hermes_home().resolve()
    default=Path(os.environ.get('XDG_DATA_HOME') or Path.home()/'.local/share')/'opencode'/'opencode.db'
    source_path=Path(args.source).expanduser().resolve() if args.source else default.resolve()
    if not source_path.is_file():
        print('OpenCode database not found: '+str(source_path),file=sys.stderr);return 1
    root=home/'imports'/'opencode-sync'
    key=hashlib.sha256(str(source_path).encode()).hexdigest()[:12]
    state_path=root/('state-'+key+'.json')
    report={'ok':True,'dry_run':bool(args.dry_run),'source':str(source_path),'new':0,'updated':0,'unchanged':0,
            'conflicts':[],'busy':[],'errors':[],'segments_written':0}
    source=None;db=None;snapshots={};lock=None
    say=lambda message: print(message,flush=True) if not args.json else None
    try:
        if args.session:
            from .model import SOURCE_ID
            if not SOURCE_ID.fullmatch(args.session):raise ValueError('Invalid literal OpenCode session ID; it was not normalized')
        if not args.dry_run:
            root.mkdir(mode=0o700,parents=True,exist_ok=True)
            lock=(root/'sync.lock').open('a')
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:raise ValueError('Another OpenCode sync is already running')
        cached=json.loads(state_path.read_text()) if state_path.exists() else {}
        source=SourceReader(source_path)
        endpoint=None
        if not args.dry_run:
            receipt=discover_gateway_endpoint(home,timeout=5)
            if not attachable(receipt):receipt=ensure_gateway_runtime(home,timeout=40)
            if not attachable(receipt):raise ValueError('Hermes profile gateway is unavailable: '+str(receipt))
            endpoint=receipt.endpoint
            health=native_call(home,endpoint,'health')
            if not health.get('ok') or health['profile_id']!=str(home):raise ValueError('Sync endpoint profile mismatch')
        if (home/'state.db').is_file():
            db=SessionDB(db_path=home/'state.db',read_only=True)
        state,assets,snapshot_paths,groups=initial_state(home,db,source,cached)
        entries=state['entries'];converter=SyncConverter(root,assets)
        directory=os.path.realpath(os.path.expanduser(args.directory)) if args.directory else None
        selected=[sid for sid,meta in source.sessions.items()
                  if (not args.session or sid==args.session) and
                     (directory is None or os.path.realpath(meta['directory'])==directory)]
        if args.session and args.session not in source.sessions:raise ValueError('Source session not found: '+args.session)
        say(('Previewing' if args.dry_run else 'Checking')+f' {len(selected)} OpenCode sessions...')
        run_id=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
        for i,sid in enumerate(sorted(selected,key=lambda s:(source.sessions[s]['time_created'],s)),1):
            entry=entries.get(sid) or {'ids':[],'baselines':{},'signature':None}
            try:
                if entry.get('signature') is None and sid in snapshot_paths:
                    path=snapshot_paths[sid]
                    if path not in snapshots:snapshots[path]=SourceReader(path)
                    if sid in snapshots[path].sessions:entry['signature']=snapshots[path].signature(sid)
                signature=source.signature(sid)
                if signature==entry.get('signature'):
                    report['unchanged']+=1;entries[sid]=entry
                    continue
                baseline=entry.get('baselines') or {}
                native_rows=groups.get(sid) or []
                # A native branch/compaction not owned by this importer is a
                # conflict, not permission to rewrite the expanded lineage.
                if any(hid not in entry['ids'] for hid,_,_ in native_rows):
                    raise ValueError('hermes_modified: extra Hermes branch/segment')
                for hid in entry['ids']:
                    row=db.get_session(hid) if db is not None else None
                    if row is None:continue
                    config=json.loads(row.get('model_config') or '{}')
                    expected=(config.get('_opencode_sync') or {}).get('message_fingerprint') or baseline.get(hid)
                    if expected is None or fingerprint(clean_messages(db,hid))!=expected:
                        raise ValueError('hermes_modified: '+hid)
                new=not entry['ids']
                if args.dry_run:
                    report['new' if new else 'updated']+=1
                    say(('NEW    ' if new else 'UPDATE ')+str(source.sessions[sid].get('title') or sid))
                    continue
                prepared=__import__(__package__+'.model',fromlist=['payloads']).payloads(source,sid,entry,db,converter,signature)
                ids=prepared[0]['all_ids']
                for body in prepared:
                    body['all_baselines']=dict(entry.get('baselines') or {})
                    result=native_call(home,endpoint,'segment',body)
                    actual=db.get_session(result['id'])
                    if actual is None or fingerprint(clean_messages(db,result['id']))!=result['fingerprint']:
                        raise ValueError('Owner update failed readback verification: '+result['id'])
                    entry.setdefault('baselines',{})[result['id']]=result['fingerprint']
                    entry['ids']=ids
                    entries[sid]=entry
                    atomic_json(state_path,state)
                    if result['status']!='unchanged':report['segments_written']+=1
                entry['signature']=signature;entries[sid]=entry
                atomic_json(state_path,state)
                report['new' if new else 'updated']+=1
                say(('NEW    ' if new else 'UPDATED ')+str(source.sessions[sid].get('title') or sid))
            except (ValueError,RuntimeError,TypeError,KeyError,sqlite3.Error) as exc:
                detail=str(exc)
                item={'source_id':sid,'reason':detail}
                if any(code in detail for code in ('session_busy','unknown_execution','lease','locked')):
                    report['busy'].append(item);say('BUSY   '+sid+' (retry after its Hermes turn finishes)')
                elif any(code in detail for code in ('hermes_modified','workspace_changed','deleted_in_hermes')):
                    report['conflicts'].append(item);say('KEPT   '+sid+' — '+detail)
                else:
                    report['errors'].append(item);say('ERROR  '+sid+' — '+detail)
            finally:
                if i%250==0:say(f'Checked {i}/{len(selected)} sessions')
        report['ok']=not report['errors'] and not report['conflicts'] and not report['busy']
        if not args.dry_run:
            atomic_json(state_path,state)
            atomic_json(root/'runs'/(run_id+'.json'),report)
            atomic_json(root/'last-run.json',report)
        if args.json:print(json.dumps(report,ensure_ascii=False))
        else:
            print(f"{'Would import' if args.dry_run else 'Imported'} {report['new']} new; "
                  f"{'would update' if args.dry_run else 'updated'} {report['updated']}; "
                  f"unchanged {report['unchanged']}; preserved conflicts {len(report['conflicts'])}; "
                  f"busy {len(report['busy'])}; errors {len(report['errors'])}.")
            if report['conflicts']:print('Hermes-modified chats were left intact. Details: '+str(root/'last-run.json'))
        return 0 if report['ok'] else 2
    except (OSError,ValueError,RuntimeError,sqlite3.Error) as exc:
        print('OpenCode sync: '+str(exc),file=sys.stderr);return 1
    finally:
        if db is not None:db.close()
        if source is not None:source.close()
        for reader in snapshots.values():reader.close()
        if lock is not None:lock.close()
