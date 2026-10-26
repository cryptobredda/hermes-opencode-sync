#!/usr/bin/env python3
"""Offline tests only: real SessionDB instances under TMPDIR, never live data."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import types
import unittest

try:
    from hermes_state import SessionDB
    from gateway.session_contract import Principal
    from hermes_state_runtime import RuntimeStoreError
    from hermes_state_errors import SessionTurnLeaseLostError
except ModuleNotFoundError as exc:
    raise unittest.SkipTest('Hermes integration tests require an installed Hermes SDK: '+str(exc))

ROOT=Path(__file__).resolve().parents[1]
NAME='opencode_sync_offline_tests'
spec=importlib.util.spec_from_file_location(NAME,ROOT/'__init__.py',submodule_search_locations=[str(ROOT)])
pkg=importlib.util.module_from_spec(spec);sys.modules[NAME]=pkg;spec.loader.exec_module(pkg)
engine=__import__(NAME+'.engine',fromlist=['*'])
model=__import__(NAME+'.model',fromlist=['*'])
client=__import__(NAME+'.client',fromlist=['*'])


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR'))
        self.root=Path(self.tmp.name)
        self.db=SessionDB(db_path=self.root/'state.db')
        self.actor=Principal('uid:fixture',str(self.root),frozenset({'session:create','session:control'}),'fixture')
        self.source='ses_FixtureAlpha'
        self.meta={'id':self.source,'time_created':1790000000000}
        self.sid=model.codec.hermes_id(self.meta)
        self.next=model.codec.hermes_id(self.meta,1)
        self.addCleanup(self.tmp.cleanup);self.addCleanup(self.db.close)

    def message(self,mid,text,role='user'):
        return {'role':role,'content':text,'timestamp':1790000000.0,'active':1,'compacted':0,
                'message_uid':model.codec.uid(self.source+':'+mid+':0'),
                'display_metadata':{'opencode':{'message_id':mid,'projection_index':0,'type':role}}}

    def body(self,messages,version='one',sid=None,ids=None,parent=None,baselines=None):
        target=sid or self.sid
        return {'source_id':self.source,'source_signature':hashlib.sha256(version.encode()).hexdigest(),
          'all_ids':ids or [self.sid],'all_baselines':baselines or {},'expected_fingerprint':None,
          'session':{'id':target,'source':'cli','cwd':str(self.root),'model':None,
             'started_at':1790000000.0,'ended_at':1790000100.0,'end_reason':'opencode_import',
             'parent_session_id':parent,'title':'[OpenCode] Fixture ['+target[-10:]+']',
             'model_config':{'_opencode_import':{'tool':'opencode','source_session_id':self.source,
                'original_title':'Fixture','segment':2 if parent else 1}},'messages':messages}}

    def apply(self,body):
        return self.db._execute_write(lambda c:engine.apply_on_conn(self.db,c,body,self.actor,self.root/'backups'))

    def current(self,sid=None):
        return self.db.get_messages(sid or self.sid,include_inactive=True)

    def test_import_append_repeat_keeps_one_session_and_existing_row_ids(self):
        first=[self.message('m1','Question'),self.message('m2','Answer','assistant')]
        self.assertEqual(self.apply(self.body(first))['status'],'imported')
        ids=[m['id'] for m in self.current()]
        changed=first+[self.message('m3','Next question')]
        result=self.apply(self.body(changed,'two'))
        self.assertEqual(result['status'],'updated')
        self.assertEqual([m['id'] for m in self.current()][:len(ids)],ids)
        self.assertEqual(len(self.current()),3)
        repeated=self.apply(self.body(changed,'two'))
        self.assertEqual(repeated['status'],'unchanged')
        self.assertEqual(self.db.session_count(include_archived=True),1)

    def test_updated_source_message_is_replaced_without_duplicate_uid(self):
        first=[self.message('m1','Streaming placeholder','assistant')]
        self.apply(self.body(first));old_id=self.current()[0]['id']
        final=[self.message('m1','Completed response','assistant')]
        self.apply(self.body(final,'two'))
        self.assertEqual(self.current()[0]['content'],'Completed response')
        self.assertEqual(self.current()[0]['id'],old_id)
        self.assertEqual(len(self.current()),1)

    def test_hermes_continuation_is_preserved_and_sync_refused(self):
        first=[self.message('m1','From OpenCode')]
        self.apply(self.body(first))
        self.db.append_message(self.sid,'user','Hermes-only work')
        before=model.fingerprint(self.current())
        with self.assertRaisesRegex(engine.SyncRefusal,'hermes_modified'):
            self.apply(self.body(first+[self.message('m2','More OpenCode')],'two'))
        self.assertEqual(model.fingerprint(self.current()),before)
        self.assertEqual(self.current()[-1]['content'],'Hermes-only work')

    def test_running_turn_lease_prevents_update(self):
        first=[self.message('m1','Question')];self.apply(self.body(first))
        self.db._execute_write(lambda c:c.execute('INSERT INTO session_turn_leases VALUES(?,?,?,?)',
            (self.sid,'fixture',time.time(),time.time()+300)))
        with self.assertRaises((SessionTurnLeaseLostError,RuntimeStoreError)):
            self.apply(self.body(first+[self.message('m2','Later')],'two'))
        self.assertEqual(len(self.current()),1)

    def test_non_import_and_forged_source_identity_are_refused(self):
        self.db.create_session(self.sid,source='cli')
        with self.assertRaisesRegex(engine.SyncRefusal,'not_an_opencode_import'):
            self.apply(self.body([self.message('m1','Do not write')]))
        self.assertEqual(self.current(),[])
        forged=self.body([self.message('m1','Bad')]);forged['session']['messages'][0]['message_uid']='not-a-source-uid'
        with self.assertRaisesRegex(engine.SyncRefusal,'foreign_message_uid'):
            self.apply(forged)

    def test_operator_title_pin_and_archive_are_not_overwritten(self):
        first=[self.message('m1','Question')];self.apply(self.body(first))
        self.db._execute_write(lambda c:c.execute("UPDATE sessions SET title='My Hermes title',title_source='user',pinned=1,archived=1 WHERE id=?",(self.sid,)))
        self.apply(self.body(first+[self.message('m2','Updated')],'two'))
        row=self.db.get_session(self.sid)
        self.assertEqual(row['title'],'My Hermes title');self.assertEqual(row['pinned'],1);self.assertEqual(row['archived'],1)
        self.assertTrue(list((self.root/'backups').rglob('*.json.gz')))

    def test_continuation_is_linked_without_replacing_original_session(self):
        first=[self.message('m1','First segment')];self.apply(self.body(first))
        body=self.body([self.message('m2','Next segment')],'two',sid=self.next,ids=[self.sid,self.next],parent=self.sid,
                       baselines={self.sid:model.fingerprint(first)})
        self.apply(body)
        self.assertEqual(self.db.get_compression_lineage(self.next),[self.sid,self.next])
        self.assertEqual(self.current()[0]['content'],'First segment')

    def test_bound_local_policy_advances_to_new_segment_in_same_transaction(self):
        from hermes_state_local import POLICY_PREFIX
        from hermes_state_local_migration import LEGACY_PREFIX
        from hermes_state_local_lineage import local_physical_target
        first=[self.message('m1','First segment')];self.apply(self.body(first))
        route='local:fixture';entry={'session_id':self.sid}
        receipt={'session_id':self.sid,'legacy_session_id':self.sid,'profile_id':str(self.root),
                 'principal_id':self.actor.subject,'request_id':'fixture','route':route,'entry':entry,'lineage':[self.sid]}
        legacy={k:receipt[k] for k in ('session_id','legacy_session_id','profile_id','principal_id')}
        def bind(c):
            c.execute('UPDATE sessions SET session_key=?,chat_id=?,user_id=? WHERE id=?',(route,self.sid,self.actor.subject,self.sid))
            c.execute('INSERT INTO state_meta VALUES(?,?)',(POLICY_PREFIX+self.sid,json.dumps(receipt)))
            c.execute('INSERT INTO state_meta VALUES(?,?)',(LEGACY_PREFIX+self.sid,json.dumps(legacy)))
            c.execute('INSERT INTO gateway_routing VALUES(?,?,?,?)',('',route,json.dumps(entry),time.time()))
        self.db._execute_write(bind)
        result=self.apply(self.body([self.message('m2','Second segment')],'two',sid=self.next,ids=[self.sid,self.next],
            parent=self.sid,baselines={self.sid:model.fingerprint(first)}))
        self.assertTrue(result['advanced'])
        with self.db._read_ctx() as c:self.assertEqual(local_physical_target(c,self.sid),self.next)
        self.assertEqual(self.db.get_session(self.next)['chat_id'],self.sid)

    def test_interrupted_multi_segment_import_is_not_marked_complete(self):
        first=[self.message('m1','First')]
        self.apply(self.body(first,ids=[self.sid,self.next]))
        cached={'entries':{self.source:{'ids':[self.sid,self.next],'baselines':{self.sid:model.fingerprint(first)},'signature':None}}}
        state,_,_,_=client.initial_state(self.root,self.db,types.SimpleNamespace(path=self.root/'source.db'),cached)
        self.assertIsNone(state['entries'][self.source]['signature'])

    def test_new_segment_inherits_operator_flags_model_and_title(self):
        first=[self.message('m1','First segment')];self.apply(self.body(first))
        self.db._execute_write(lambda c:c.execute("UPDATE sessions SET title='My custom title',title_source='user',pinned=1,archived=1,hidden=1,model='my-Hermes-model' WHERE id=?",(self.sid,)))
        self.apply(self.body([self.message('m2','Second segment')],'two',sid=self.next,ids=[self.sid,self.next],
            parent=self.sid,baselines={self.sid:model.fingerprint(first)}))
        row=self.db.get_session(self.next)
        self.assertEqual((row['pinned'],row['archived'],row['hidden']),(1,1,1))
        self.assertEqual(row['model'],'my-Hermes-model')
        self.assertTrue(row['title'].startswith('My custom title #'))
        self.assertEqual(row['title_source'],'user')

    def test_invalid_segment_path_is_refused_before_any_write(self):
        body=self.body([]);body['all_ids']=['../not-a-session'];body['session']['id']='../not-a-session'
        with self.assertRaisesRegex(engine.SyncRefusal,'invalid_segment_ids'):
            self.apply(body)
        self.assertEqual(self.db.session_count(include_archived=True),0)

    def test_fresh_profile_dry_run_needs_no_hermes_database_or_gateway(self):
        import argparse
        import contextlib
        import io
        import sqlite3
        from hermes_constants import set_hermes_home_override,reset_hermes_home_override
        source_path=self.root/'opencode.db'
        c=sqlite3.connect(source_path)
        c.executescript('CREATE TABLE session_v2(id TEXT PRIMARY KEY,title TEXT,directory TEXT,time_created INTEGER,time_updated INTEGER); CREATE TABLE session_message(id TEXT PRIMARY KEY,session_id TEXT,type TEXT,seq INTEGER,time_created INTEGER,time_updated INTEGER,data TEXT);')
        c.execute('INSERT INTO session_v2 VALUES(?,?,?,?,?)',(self.source,'New chat',str(self.root),1790000000000,1790000000000))
        c.commit();c.close()
        fresh=self.root/'fresh-profile'
        token=set_hermes_home_override(str(fresh))
        try:
            output=io.StringIO()
            args=argparse.Namespace(source=str(source_path),directory=None,session=None,dry_run=True,json=True)
            with contextlib.redirect_stdout(output):result=client.main(args)
            self.assertEqual(result,0)
            report=json.loads(output.getvalue())
            self.assertEqual(report['new'],1)
            self.assertFalse(fresh.exists())
        finally:reset_hermes_home_override(token)

    def test_invalid_literal_id_is_not_normalized(self):
        body=self.body([]);body['source_id']='ses_bad_id'
        with self.assertRaisesRegex(engine.SyncRefusal,'invalid_source_id'):
            self.apply(body)


if __name__=='__main__':unittest.main()
