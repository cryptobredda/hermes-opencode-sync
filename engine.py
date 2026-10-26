"""Owner-process, guarded updates of import-owned rows. Never opens another writer."""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
import gzip
import hashlib
import json
from pathlib import Path
import re
import time

from .model import codec, fingerprint, SOURCE_ID

class SyncRefusal(ValueError):
    pass

ACTIVE_UPDATES = 0


def messages_on_conn(db,conn,sid):
    return [db._row_to_message_dict(r,warn_context='OpenCode sync',summary_flag=True)
            for r in conn.execute('SELECT * FROM messages WHERE session_id=? ORDER BY id',(sid,))]


def imported(row,source_id):
    config=json.loads(row['model_config'] or '{}')
    origin=config.get('_opencode_import') or {}
    if origin.get('tool')!='opencode' or origin.get('source_session_id')!=source_id:
        raise SyncRefusal('not_an_opencode_import')
    return config


def verify_group(db,conn,body):
    expected=body.get('all_baselines') or {}
    known=set(body['all_ids'])
    rows=conn.execute("SELECT * FROM sessions WHERE json_extract(model_config,'$._opencode_import.source_session_id')=?",(body['source_id'],)).fetchall()
    for row in rows:
        if row['id'] not in known:raise SyncRefusal('hermes_modified: an additional Hermes branch/segment exists')
        config=imported(row,body['source_id'])
        baseline=(config.get('_opencode_sync') or {}).get('message_fingerprint') or expected.get(row['id'])
        actual=fingerprint(messages_on_conn(db,conn,row['id']))
        if baseline is None or actual!=baseline:
            raise SyncRefusal('hermes_modified: transcript differs from its last imported version')
    return rows


def insert_rows(db,conn,sid,messages):
    copies=[dict(m) for m in messages]
    db._insert_message_rows(conn,sid,copies,prune_checkpoints=False)
    for original,copy in zip(messages,copies):
        conn.execute('UPDATE messages SET active=?,compacted=? WHERE id=?',
                     (int(original.get('active',1)),int(original.get('compacted',0)),copy['_row_id']))


def update_rows(db,conn,sid,old,new):
    old_ids=[m.get('message_uid') for m in old]
    new_ids=[m.get('message_uid') for m in new]
    if old_ids!=new_ids[:len(old_ids)]:
        # Ordering edits and segment reflow require fresh physical row IDs. The
        # logical IDs and message_uids stay stable; a revision fence invalidates
        # stale rewind/display controls before a viewer can use them.
        conn.execute('DELETE FROM messages WHERE session_id=?',(sid,))
        insert_rows(db,conn,sid,new)
        return True
    changed=False
    for previous,desired in zip(old,new):
        body_keys=('role','content','tool_call_id','tool_calls','tool_name','timestamp','reasoning_content','finish_reason')
        body_changed=any(previous.get(k)!=desired.get(k) for k in body_keys)
        flags_changed=any(int(previous.get(k,default))!=int(desired.get(k,default)) for k,default in [('active',1),('compacted',0)])
        if not body_changed and not flags_changed:continue
        changed=True
        if body_changed:
            calls=desired.get('tool_calls')
            calls=json.dumps(calls,ensure_ascii=False) if calls is not None and not isinstance(calls,str) else calls
            conn.execute('''UPDATE messages SET role=?,content=?,tool_call_id=?,tool_calls=?,tool_name=?,timestamp=?,
                reasoning_content=?,finish_reason=?,display_metadata=?,active=?,compacted=?,
                display_identity=NULL,display_order=NULL,tool_call_uids=NULL,tool_call_uid=NULL WHERE id=?''',
                (desired['role'],db._encode_content(desired.get('content')),desired.get('tool_call_id'),calls,
                 desired.get('tool_name'),desired['timestamp'],desired.get('reasoning_content'),desired.get('finish_reason'),
                 db._encode_display_metadata(desired.get('display_metadata')),int(desired.get('active',1)),
                 int(desired.get('compacted',0)),previous['id']))
        else:
            conn.execute('UPDATE messages SET active=?,compacted=? WHERE id=?',
                         (int(desired.get('active',1)),int(desired.get('compacted',0)),previous['id']))
    if len(new)>len(old):
        insert_rows(db,conn,sid,new[len(old):]);changed=True
    return changed


def validate_body(db,body):
    if not isinstance(body,dict) or set(body)!={'source_id','source_signature','session','expected_fingerprint','all_ids','all_baselines'}:
        raise SyncRefusal('invalid_payload')
    if not isinstance(body['source_id'],str) or not SOURCE_ID.fullmatch(body['source_id']):raise SyncRefusal('invalid_source_id')
    if not isinstance(body['source_signature'],str) or not re.fullmatch('[0-9a-f]{64}',body['source_signature']):raise SyncRefusal('invalid_source_signature')
    ids=body['all_ids']
    if not isinstance(ids,list) or not ids or len(ids)>1000 or not all(isinstance(x,str) and re.fullmatch(r'\d{8}_\d{6}_[0-9a-f]{12}',x) for x in ids) or len(ids)!=len(set(ids)):
        raise SyncRefusal('invalid_segment_ids')
    payload=body['session']
    normalized,errors=db._validate_import_payload([payload])
    if errors:raise SyncRefusal(str(errors[0]))
    clean=normalized[0]['session'];messages=normalized[0]['messages']
    sid=clean['id']
    if sid not in ids or clean.get('source')!='cli':raise SyncRefusal('invalid_import_scope')
    origin=(json.loads(clean['model_config'] or '{}').get('_opencode_import') or {})
    if origin.get('tool')!='opencode' or origin.get('source_session_id')!=body['source_id']:raise SyncRefusal('invalid_provenance')
    parent=clean.get('parent_session_id')
    if parent and parent not in ids:raise SyncRefusal('foreign_parent')
    uids=[]
    for message in messages:
        meta=message.get('display_metadata') or {}
        if isinstance(meta,str):meta=json.loads(meta)
        source=meta.get('opencode') or {}
        mid=source.get('message_id');index=source.get('projection_index')
        if not isinstance(mid,str) or not isinstance(index,int):raise SyncRefusal('message_missing_source_identity')
        expected=codec.uid(body['source_id']+':'+mid+':'+str(index))
        if message.get('message_uid')!=expected:raise SyncRefusal('foreign_message_uid')
        uids.append(expected)
    if len(uids)!=len(set(uids)):raise SyncRefusal('duplicate_message_uid')
    return clean,messages


def apply_on_conn(db,conn,body,actor,backup_root,logical_ids=()):
    from hermes_state_mutation_guards import require_idle
    from hermes_state_mutation_binding import import_history_control
    from hermes_state_mutation_retirement import RETIRED_PREFIX
    clean,messages=validate_body(db,body)
    sid=clean['id']
    group=verify_group(db,conn,body)
    guards=set(body['all_ids'])|set(logical_ids)|{r['chat_id'] for r in group if r['chat_id']}
    existing_ids=[x for x in guards if conn.execute('SELECT 1 FROM sessions WHERE id=?',(x,)).fetchone()]
    require_idle(db,conn,existing_ids)
    row=conn.execute('SELECT * FROM sessions WHERE id=?',(sid,)).fetchone()
    if conn.execute('SELECT 1 FROM state_meta WHERE key=?',(RETIRED_PREFIX+sid,)).fetchone():raise SyncRefusal('deleted_in_hermes')
    desired_hash=fingerprint(messages)
    old=messages_on_conn(db,conn,sid) if row is not None else []
    previous_hash=fingerprint(old)
    config=imported(row,body['source_id']) if row is not None else {}
    origin=config.get('_opencode_import') or {}
    if row is not None and row['cwd']!=clean.get('cwd') and row['session_key']:
        raise SyncRefusal('workspace_changed: a bound Hermes workspace must be moved explicitly')
    stamp=config.get('_opencode_sync') or {}
    metadata_changed=stamp.get('source_signature')!=body['source_signature']
    changed=previous_hash!=desired_hash
    if row is not None and not changed and not metadata_changed:
        return {'ok':True,'status':'unchanged','id':sid,'fingerprint':desired_hash,'messages':len(messages),'advanced':False}
    backup=None
    if row is not None:
        backup_dir=Path(backup_root)/body['source_id'];backup_dir.mkdir(mode=0o700,parents=True,exist_ok=True)
        backup=backup_dir/(sid+'-'+previous_hash[:16]+'.json.gz')
        if not backup.exists():
            snapshot={'session':dict(row),'messages':old}
            with backup.open('xb') as f:f.write(gzip.compress(json.dumps(snapshot,ensure_ascii=False).encode()))
            backup.chmod(0o600)
    fresh_config=json.loads(clean['model_config'] or '{}')
    merged={**config,**{k:v for k,v in fresh_config.items() if k=='_opencode_import'}}
    merged['_opencode_import']={**origin,**fresh_config['_opencode_import']}
    merged['_opencode_sync']={'version':1,'source_signature':body['source_signature'],
                             'message_fingerprint':desired_hash,'synced_at':time.time()}
    clean['model_config']=json.dumps(merged,ensure_ascii=False)
    advanced=False
    if row is None:
        parent=clean.get('parent_session_id')
        parent_row=conn.execute('SELECT * FROM sessions WHERE id=?',(parent,)).fetchone() if parent else None
        if parent and parent_row is None:raise SyncRefusal('missing_parent_segment')
        if parent_row:
            parent_config=imported(parent_row,body['source_id'])
            clean['model_config']=json.dumps({**parent_config,**merged},ensure_ascii=False)
            if parent_row['title_source']=='user':
                base=(parent_config.get('_opencode_sync') or {}).get('hermes_title') or parent_row['title']
                user_title=base+' #'+str(fresh_config['_opencode_import']['segment'])
                suffix=2
                while conn.execute('SELECT 1 FROM sessions WHERE title=?',(user_title,)).fetchone():
                    user_title=base+' #'+str(fresh_config['_opencode_import']['segment'])+' ('+str(suffix)+')';suffix+=1
                clean['title']=user_title
                child_config=json.loads(clean['model_config'])
                child_config['_opencode_sync']['hermes_title']=base
                clean['model_config']=json.dumps(child_config,ensure_ascii=False)
        import_history_control(conn,actor,(sid,))
        db._import_session_row(conn,clean,messages,sid)
        if parent:
            conn.execute('UPDATE sessions SET parent_session_id=? WHERE id=?',(parent,sid))
            cols=('pinned','archived','hidden','auto_archived','model','display_name','title_source')
            conn.execute('UPDATE sessions SET '+','.join(c+'=?' for c in cols)+' WHERE id=?',tuple(parent_row[c] for c in cols)+(sid,))
            conn.execute("UPDATE sessions SET end_reason='compression',ended_at=COALESCE(ended_at,?) WHERE id=?",(clean['ended_at'],parent))
            if parent_row['chat_id']:
                # Same transaction as the published child: preserve canonical
                # owner identity/policy/FIFO, as native compression does.
                cols=('session_key','chat_id','user_id','chat_type','origin_json','profile_name')
                conn.execute('UPDATE sessions SET '+','.join(c+'=?' for c in cols)+' WHERE id=?',
                             tuple(parent_row[c] for c in cols)+(sid,))
                from hermes_state_local_lineage import advance_local_target
                advance_local_target(conn,parent,sid)
                advanced=True
    else:
        changed = update_rows(db,conn,sid,old,messages) or changed
        # Keep operator pins, archives, hidden flags, route, launch model and
        # policy. Only update a source-owned title (not a Hermes user rename).
        title=row['title']
        old_base='[OpenCode] '+str(origin.get('original_title') or '')+' ['+body['source_id'][-10:]+']'
        if row['title_source']!='user' and (title==old_base or str(title).startswith(old_base+' (import segment ')):
            title=clean.get('title')
        conn.execute('''UPDATE sessions SET model_config=?,title=?,cwd=?,
            end_reason=CASE WHEN session_key IS NULL THEN ? ELSE end_reason END,
            ended_at=CASE WHEN session_key IS NULL THEN ? ELSE ended_at END,
            input_tokens=?,output_tokens=?,cache_read_tokens=?,cache_write_tokens=?,reasoning_tokens=? WHERE id=?''',
            (clean['model_config'],title,clean.get('cwd'),clean.get('end_reason'),clean.get('ended_at'),
             clean.get('input_tokens') or 0,clean.get('output_tokens') or 0,clean.get('cache_read_tokens') or 0,
             clean.get('cache_write_tokens') or 0,clean.get('reasoning_tokens') or 0,sid))
    live=[m for m in messages if int(m.get('active',1))]
    calls=sum(len(m.get('tool_calls') or []) for m in live)
    conn.execute('UPDATE sessions SET message_count=?,tool_call_count=? WHERE id=?',(len(live),calls,sid))
    if changed:
        for affected in {sid,*logical_ids}:
            conn.execute('UPDATE sessions SET runtime_revision=runtime_revision+1,runtime_generation=runtime_generation+1 WHERE id=?',(affected,))
    # Verification uses the exact writer connection, before commit.
    if fingerprint(messages_on_conn(db,conn,sid))!=desired_hash:raise SyncRefusal('write_verification_failed')
    return {'ok':True,'status':'imported' if row is None else 'updated','id':sid,'fingerprint':desired_hash,
            'messages':len(messages),'backup':str(backup) if backup else None,'advanced':advanced,'changed':changed}


async def _apply_owned(authority,actor,body,backup_root):
    from gateway.session_contract import SessionRef
    from hermes_state_mutation_binding import authorize_history
    authority._require_admission_open()
    # Authorize only existing, proven OpenCode imports in this actor's profile.
    group=[]
    with authority.db._read_ctx() as conn:
        for sid in body['all_ids']:
            row=conn.execute('SELECT * FROM sessions WHERE id=?',(sid,)).fetchone()
            if row is None:continue
            imported(row,body['source_id']);group.append(dict(row))
            if not row['session_key'] and not authorize_history(conn,actor,sid):raise SyncRefusal('permission_denied')
    owner_ids={r['chat_id'] or r['id'] for r in group}
    lives={sid:authority.sessions[sid] for sid in owner_ids if sid in authority.sessions}
    for sid in lives:
        authority.authorize(actor,SessionRef(authority.profile_id,sid),'session:control')
    async with AsyncExitStack() as stack:
        for sid in sorted(lives):await stack.enter_async_context(lives[sid].claim_gate)
        for sid in sorted(lives):await stack.enter_async_context(lives[sid].mutation_lock)
        def write(conn):
            from hermes_state_runtime import _epoch
            _epoch(conn,authority.epoch)
            return apply_on_conn(authority.db,conn,body,actor,backup_root,tuple(lives))
        result=await asyncio.to_thread(authority.db._execute_write,write)
        if result.get('advanced'):
            from gateway.session_local_recovery import restore_local_session
            for sid in lives:restore_local_session(authority,sid)
        if result.get('changed'):
            for sid,live in lives.items():
                authority.runner._evict_cached_agent(live.route)
                row=authority.db.get_session(sid)
                live.event_stream.publish(sid,{'operation':'opencode_sync','session_id':sid,'target_session_id':result['id'],
                    'revision':row['runtime_revision'],'execution_generation':row['runtime_generation']},event_type='session.updated')
        return result


async def apply_owned(authority,actor,body,backup_root):
    global ACTIVE_UPDATES
    ACTIVE_UPDATES+=1
    try:
        return await _apply_owned(authority,actor,body,backup_root)
    finally:
        ACTIVE_UPDATES-=1
