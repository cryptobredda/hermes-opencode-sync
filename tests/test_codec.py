import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[1]
NAME='opencode_codec_tests'
spec=importlib.util.spec_from_file_location(NAME,ROOT/'__init__.py',submodule_search_locations=[str(ROOT)])
pkg=importlib.util.module_from_spec(spec);sys.modules[NAME]=pkg;spec.loader.exec_module(pkg)
model=__import__(NAME+'.model',fromlist=['*'])


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR'))
        self.root=Path(self.temp.name);self.path=self.root/'opencode.db'
        conn=sqlite3.connect(self.path)
        conn.executescript('''CREATE TABLE session_v2(id TEXT PRIMARY KEY,title TEXT,directory TEXT,time_created INTEGER,time_updated INTEGER);
          CREATE TABLE session_message(id TEXT PRIMARY KEY,session_id TEXT,type TEXT,seq INTEGER,time_created INTEGER,time_updated INTEGER,data TEXT);
          CREATE TABLE credential(id TEXT,value TEXT);''')
        conn.execute('INSERT INTO session_v2 VALUES(?,?,?,?,?)',('ses_Example','Example',str(self.root),1790000000000,1790000001000))
        user={'text':'Example question','time':{'created':1790000000000}}
        assistant={'content':[{'type':'text','text':'Example answer'}],'time':{'created':1790000001000}}
        for index,kind,value in [(0,'user',user),(1,'assistant',assistant)]:
            conn.execute('INSERT INTO session_message VALUES(?,?,?,?,?,?,?)',('m'+str(index),'ses_Example',kind,index,1790000000000+index,1790000000000+index,json.dumps(value)))
        conn.execute('INSERT INTO credential VALUES(?,?)',('fixture','MUST NOT BE READ'))
        conn.commit();conn.close();self.source=model.SourceReader(self.path)
        self.addCleanup(self.temp.cleanup);self.addCleanup(lambda:self.source.close())

    def test_only_history_tables_can_be_read(self):
        with self.assertRaises(sqlite3.DatabaseError):self.source.conn.execute('SELECT value FROM credential').fetchall()
        self.assertIn('ses_Example',self.source.sessions)

    def test_source_connection_is_read_only(self):
        with self.assertRaises(sqlite3.DatabaseError):self.source.conn.execute("UPDATE session_v2 SET title='changed'")

    def test_source_signature_is_repeatable_and_literal_identity_preserved(self):
        a=self.source.signature('ses_Example');b=self.source.signature('ses_Example')
        self.assertEqual(a,b);self.assertNotIn('ses_example',self.source.sessions)

    def test_conversion_preserves_text_and_deterministic_message_uids(self):
        converter=model.SyncConverter(self.root/'imports',{})
        messages=converter.messages(self.source,'ses_Example')
        self.assertEqual([m['content'] for m in messages],['Example question','Example answer'])
        self.assertEqual(messages[0]['message_uid'],model.codec.uid('ses_Example:m0:0'))
        self.assertEqual(len({m['message_uid'] for m in messages}),len(messages))

    def test_artifacts_are_content_addressed_and_checked_not_overwritten(self):
        c=model.SyncConverter(self.root/'imports',{})
        path=Path(c.save_bytes(b'example bytes','text/plain','example.txt'))
        self.assertEqual(c.save_bytes(b'example bytes','text/plain','example.txt'),str(path))
        path.write_bytes(b'edited by user')
        with self.assertRaisesRegex(ValueError,'refusing to overwrite'):
            c.save_bytes(b'example bytes','text/plain','example.txt')

    def test_unknown_literal_session_id_is_refused_not_normalized(self):
        self.source.close();self.source.close=lambda:None
        conn=sqlite3.connect(self.path);conn.execute("UPDATE session_v2 SET id='ses_bad_id'");conn.commit();conn.close()
        with self.assertRaisesRegex(ValueError,'literal session ID'):
            model.SourceReader(self.path)


if __name__=='__main__':unittest.main()
