"""Read-only OpenCode snapshots and snapshot-compatible transcript conversion."""
from __future__ import annotations

import collections
from datetime import datetime, timezone
import hashlib
import json
import mimetypes
import os
from pathlib import Path
import re
import sqlite3
import uuid

from . import legacy_codec as codec

fingerprint = codec.fingerprint
SOURCE_ID = re.compile(r'ses_[A-Za-z0-9]+\Z')
META_FIELDS = ('id','title','slug','directory','parent_id','fork_session_id','fork_boundary','model',
               'time_created','time_updated','cost','tokens_input','tokens_output','tokens_reasoning',
               'tokens_cache_read','tokens_cache_write')


def read_db(path):
    conn = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=15)
    conn.row_factory=sqlite3.Row
    conn.execute('PRAGMA query_only=ON')
    return conn


class SourceReader:
    def __init__(self,path):
        self.path=Path(path).resolve()
        self.conn=read_db(self.path)
        # The source database also contains login tables. Deny reading any
        # non-history table even if a later query accidentally mentions one.
        allowed={'sqlite_master','sqlite_schema','session','session_v2','session_message','message','part','todo'}
        self.conn.set_authorizer(lambda action,table,column,database,trigger:
            sqlite3.SQLITE_DENY if action==sqlite3.SQLITE_READ and table not in allowed else sqlite3.SQLITE_OK)
        self.conn.execute('BEGIN')
        tables={r[0] for r in self.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'session' not in tables and 'session_v2' not in tables:
            self.conn.close()
            raise ValueError('Not a supported OpenCode session database: '+str(path))
        self.old={r['id']:dict(r) for r in self.conn.execute('SELECT * FROM session')} if 'session' in tables else {}
        self.new={r['id']:dict(r) for r in self.conn.execute('SELECT * FROM session_v2')} if 'session_v2' in tables else {}
        self.sessions={**self.old,**self.new}
        self.v2_ids={r[0] for r in self.conn.execute('SELECT id FROM session_message')} if 'session_message' in tables else set()
        self.has_legacy='message' in tables and 'part' in tables
        for sid in self.sessions:
            if not SOURCE_ID.fullmatch(sid):
                self.conn.close()
                raise ValueError('Unsupported literal session ID (not normalized): '+repr(sid))

    def close(self):
        self.conn.rollback();self.conn.close()

    def rows(self,sid):
        v2=list(map(dict,self.conn.execute('SELECT * FROM session_message WHERE session_id=? ORDER BY seq',(sid,)))) if sid in self.new else []
        old=[]
        if self.has_legacy:
            for row in self.conn.execute('SELECT * FROM message WHERE session_id=? ORDER BY time_created,id',(sid,)):
                if row['id'] not in self.v2_ids:
                    old.append((dict(row),[json.loads(p[0]) for p in self.conn.execute('SELECT data FROM part WHERE message_id=? ORDER BY id',(row['id'],))]))
        return v2,old

    def signature(self,sid):
        h=hashlib.sha256()
        meta={k:self.sessions[sid].get(k) for k in META_FIELDS}
        h.update(codec.dump(meta).encode());h.update(b'\n')
        if sid in self.new:
            for row in self.conn.execute('SELECT id,type,seq,data FROM session_message WHERE session_id=? ORDER BY seq',(sid,)):
                h.update(codec.dump(tuple(row)).encode());h.update(b'\n')
        if self.has_legacy:
            for row in self.conn.execute('SELECT id,data FROM message WHERE session_id=? ORDER BY time_created,id',(sid,)):
                if row['id'] in self.v2_ids:continue
                h.update(codec.dump(tuple(row)).encode());h.update(b'\n')
                for part in self.conn.execute('SELECT id,data FROM part WHERE message_id=? ORDER BY id',(row['id'],)):
                    h.update(codec.dump(tuple(part)).encode());h.update(b'\n')
        return h.hexdigest()


class SyncConverter(codec.Converter):
    def __init__(self,root,legacy_assets):
        self.root=Path(root)
        self.assets={};self.asset_references=0;self.external_logs=0
        self.counts=collections.Counter();self.legacy_assets=legacy_assets

    def save_bytes(self,data,mime,name=None,*,origin=None):
        digest=hashlib.sha256(data).hexdigest()
        ext=Path(name or '').suffix.lower()
        if not re.fullmatch(r'\.[a-z0-9]{1,10}',ext):
            ext={'text/md':'.md','text/markdown':'.md','image/jpeg':'.jpg'}.get(mime) or mimetypes.guess_extension(mime) or '.bin'
        candidate=self.legacy_assets.get(digest+ext)
        path=Path(candidate) if candidate else self.root/'assets'/(digest+ext)
        if path.exists():
            if path.stat().st_size!=len(data) or hashlib.sha256(path.read_bytes()).hexdigest()!=digest:
                raise ValueError('Imported attachment changed on disk; refusing to overwrite it: '+str(path))
        else:
            path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
            temp=path.with_suffix(path.suffix+'.'+uuid.uuid4().hex+'.new')
            with temp.open('xb') as f:f.write(data)
            temp.chmod(0o600);os.replace(temp,path)
        self.assets[str(path)]={'path':str(path),'sha256':digest,'bytes':len(data),'mime':mime}
        self.asset_references+=1
        return str(path)

    def messages(self,source,sid):
        v2,old=source.rows(sid)
        latest=max((r['seq'] for r in v2 if r['type']=='compaction' and json.loads(r['data']).get('status')=='completed'),default=-1)
        result=[]
        for row,parts in old:
            row['_active']=sid not in source.new
            result.extend(self.legacy(row,parts))
        for row in v2:
            row['_active']=row['seq']>=latest
            result.extend(self.v2(row))
        return result


def legacy_seed(home):
    entries={};assets={};snapshots={}
    for path in sorted((Path(home)/'imports').glob('opencode-*/prepared-manifest.json')):
        if not path.resolve().is_relative_to(Path(home).resolve()):continue
        manifest=json.loads(path.read_text())
        snapshot=Path(manifest['source_database'])
        for asset in manifest.get('assets',[]):
            assets[Path(asset['path']).name]=asset['path']
        for entry in manifest.get('sessions',[]):
            sid=entry['source_id']
            entries[sid]={'ids':entry['hermes_ids'],'baselines':{s['id']:s['message_fingerprint'] for s in entry['segments']},
                          'signature':None,'title':entry['title']}
            if snapshot.exists():snapshots[sid]=snapshot
    return entries,assets,snapshots


def native_groups(db):
    result={}
    if db is None:return result
    with db._read_ctx() as conn:
        for row in conn.execute("SELECT id,model_config FROM sessions WHERE model_config LIKE '%_opencode_import%'"):
            config=json.loads(row['model_config'] or '{}');origin=config.get('_opencode_import') or {}
            sid=origin.get('source_session_id')
            if origin.get('tool')!='opencode' or not isinstance(sid,str):continue
            result.setdefault(sid,[]).append((row['id'],origin,config.get('_opencode_sync') or {}))
    return result


def clean_messages(db,sid):
    return db.get_messages(sid,include_inactive=True)


def payloads(source,sid,entry,db,converter,signature):
    meta=source.sessions[sid]
    messages=converter.messages(source,sid)
    all_uids={m['message_uid'] for m in messages}
    retired=[]
    # Source-side edits/rewinds never erase the older imported bytes: keep them
    # as inactive, non-compacted archival rows (as Hermes' own rewind does).
    for existing in entry.get('ids',[]):
        if db.get_session(existing):
            for old in clean_messages(db,existing):
                if old.get('message_uid') not in all_uids:
                    retired.append({**old,'active':0,'compacted':0})
    messages=retired+messages
    chunks=[];chunk=[];size=0
    for message in messages:
        n=codec.wire_size(message)+2
        if chunk and (size+n>codec.SEGMENT_BYTES or len(chunk)>=codec.SEGMENT_MESSAGES):
            chunks.append(chunk);chunk=[];size=0
        chunk.append(message);size+=n
    if chunk or not chunks:chunks.append(chunk)
    while len(chunks)<len(entry.get('ids',[])):chunks.append([])
    ids=[codec.hermes_id(meta,i) for i in range(len(chunks))]
    known=entry.get('ids') or []
    if known and ids[:len(known)]!=known:
        raise ValueError('Imported segment mapping changed; refusing to guess a replacement ID')
    title=meta.get('title') or meta.get('slug') or sid
    base='[OpenCode] '+title+' ['+sid[-10:]+']'
    result=[]
    for i,msgs in enumerate(chunks):
        origin={'tool':'opencode','source_session_id':sid,'source_database':str(source.path),
                'original_title':title,'original_model':meta.get('model'),'original_parent_id':meta.get('parent_id'),
                'original_fork_session_id':meta.get('fork_session_id'),'segment':i+1,'segment_count':len(chunks)}
        payload={'id':ids[i],'source':'cli','model':None,'cwd':meta['directory'],
                 'title':base if i==len(chunks)-1 else base+f' (import segment {i+1}/{len(chunks)})',
                 'started_at':meta['time_created']/1000+i/1000000,'ended_at':meta['time_updated']/1000,
                 'end_reason':'compression' if i<len(chunks)-1 else 'opencode_import',
                 'parent_session_id':ids[i-1] if i else None,
                 'model_config':{'_opencode_import':origin},'messages':msgs,
                 'input_tokens':int(meta.get('tokens_input') or 0) if i==len(chunks)-1 else 0,
                 'output_tokens':int(meta.get('tokens_output') or 0) if i==len(chunks)-1 else 0,
                 'reasoning_tokens':int(meta.get('tokens_reasoning') or 0) if i==len(chunks)-1 else 0,
                 'cache_read_tokens':int(meta.get('tokens_cache_read') or 0) if i==len(chunks)-1 else 0,
                 'cache_write_tokens':int(meta.get('tokens_cache_write') or 0) if i==len(chunks)-1 else 0}
        result.append({'source_id':sid,'source_signature':signature,'session':payload,
                       'expected_fingerprint':entry.get('baselines',{}).get(ids[i]),'all_ids':ids})
    return result
