"""Snapshot-compatible OpenCode transcript conversion; stdlib only, no host bootstrap."""
from __future__ import annotations
import base64
import collections
from datetime import datetime,timezone
import hashlib
import json
import mimetypes
from pathlib import Path
import re
import uuid
from urllib.parse import unquote_to_bytes
ROOT=Path(__file__).resolve().parent
SEGMENT_BYTES=2800*1024
SEGMENT_MESSAGES=5500
BATCH_BYTES=4*1024*1024
FIELDS=("role","content","tool_call_id","tool_calls","tool_name","timestamp","reasoning_content","active","compacted","message_uid")

def dump(value, *, ascii=False):
    return json.dumps(value, ensure_ascii=ascii, sort_keys=True, separators=(',', ':'), allow_nan=False)

def wire_size(value):
    return len(json.dumps(value).encode())

def uid(value):
    return uuid.uuid5(uuid.NAMESPACE_URL, 'opencode-hermes-import-v1:' + value).hex

def fingerprint(messages):
    h = hashlib.sha256()
    for m in messages:
        selected = {k: m.get(k) for k in FIELDS}
        selected['active'] = int(m.get('active', 1))
        selected['compacted'] = int(m.get('compacted', 0))
        h.update(dump(selected).encode())
        h.update(b'\n')
    return h.hexdigest()

def hermes_id(session, segment=0):
    stamp = datetime.fromtimestamp(session['time_created'] / 1000, timezone.utc).strftime('%Y%m%d_%H%M%S')
    digest = hashlib.sha256(('opencode:' + session['id'] + ':' + str(segment)).encode()).hexdigest()[:12]
    return stamp + '_' + digest

class Converter:
    def __init__(self):
        self.assets = {}
        self.asset_references = 0
        self.external_logs = 0
        self.counts = collections.Counter()
        (ROOT / 'assets').mkdir(exist_ok=True)
        (ROOT / 'prepared').mkdir(exist_ok=True)
        (ROOT / 'source-metadata').mkdir(exist_ok=True)
        self.asset_log = (ROOT / 'asset-references.jsonl').open('w')

    def save_bytes(self, data, mime, name=None, *, origin=None):
        digest = hashlib.sha256(data).hexdigest()
        ext = Path(name or '').suffix.lower()
        if not re.fullmatch(r'\.[a-z0-9]{1,10}', ext):
            ext = { 'text/md':'.md', 'text/markdown':'.md', 'image/jpeg':'.jpg' }.get(mime) or mimetypes.guess_extension(mime) or '.bin'
        path = ROOT / 'assets' / (digest + ext)
        if not path.exists():
            path.write_bytes(data)
        self.assets[str(path)] = {'path':str(path), 'sha256':digest, 'bytes':len(data), 'mime':mime}
        self.asset_references += 1
        self.asset_log.write(dump({'path':str(path),'name':name,'mime':mime,'origin':origin})+'\n')
        return str(path)

    def attachment(self, item, origin):
        mime = item.get('mime') or item.get('mediaType') or 'application/octet-stream'
        name = item.get('name') or item.get('filename')
        value = item.get('uri') or item.get('url')
        if isinstance(item.get('data'), str):
            data = base64.b64decode(item['data'], validate=True)
            path = self.save_bytes(data, mime, name, origin=origin)
        elif isinstance(value, str) and value.startswith('data:'):
            header, payload = value.split(',', 1)
            mime = header[5:].split(';')[0] or mime
            data = base64.b64decode(payload, validate=True) if ';base64' in header else unquote_to_bytes(payload)
            path = self.save_bytes(data, mime, name, origin=origin)
        elif isinstance(value, str):
            # Preserve source URLs; never fetch remote material or copy unrelated project files.
            path = value
        else:
            raise ValueError('Unknown OpenCode attachment shape: ' + str(list(item)))
        label = name or mime
        return f'[Imported OpenCode attachment: {label}]\n' + ('MEDIA:' + path if path.startswith('/') else path)

    def row(self, role, content, source, index, **extra):
        meta = {'opencode':{'message_id':source['id'], 'type':source['type'], 'projection_index':index}}
        message = {'role':role,'content':content,'timestamp':source['time_created']/1000,
                   'message_uid':uid(source['session_id']+':'+source['id']+':'+str(index)),
                   'active':int(source.get('_active', True)),
                   'compacted':int(not source.get('_active', True)),
                   'display_metadata':meta, **extra}
        # A single giant result cannot be transported as one import row. Store
        # its complete bytes locally, without truncating or inventing tool output.
        if wire_size(message) > SEGMENT_BYTES // 2:
            if not isinstance(content, str):
                raise ValueError('Unexpected oversize non-text message')
            path = self.save_bytes(content.encode(), 'text/plain', 'historical-output.txt',
                                   origin={'message_id':source['id'],'kind':'oversize-message'})
            message['content'] = '[Complete historical OpenCode message saved as a local file; not truncated]\nMEDIA:' + path
            self.external_logs += 1
        if wire_size(message) > SEGMENT_BYTES - 65536:
            raise ValueError('Message still too large after externalizing its content')
        return message

    def assistant(self, source, data, parts):
        texts, reasoning, tools = [], [], []
        for p in parts:
            typ = p.get('type')
            if typ == 'text':
                texts.append(p.get('text',''))
            elif typ == 'reasoning':
                reasoning.append(p.get('text',''))
            elif typ == 'tool':
                state = p.get('state') or {}
                call_id = p.get('id') or p.get('callID')
                name = p.get('name') or p.get('tool')
                if not isinstance(call_id,str) or not isinstance(name,str):
                    raise ValueError('Tool call has no literal source identifier')
                args = state.get('input',{})
                args = args if isinstance(args,str) else dump(args)
                if len(args.encode()) > SEGMENT_BYTES // 2:
                    path = self.save_bytes(args.encode(),'application/json','historical-arguments.json',
                                           origin={'message_id':source['id'],'call_id':call_id})
                    args = dump({'_imported_original_arguments_file':path})
                    self.external_logs += 1
                call = {'id':call_id, 'type':'function', 'function':{'name':name,'arguments':args}}
                outputs = []
                for item in state.get('content',[]):
                    if item.get('type') == 'text':
                        outputs.append(item.get('text',''))
                    elif item.get('type') == 'file':
                        outputs.append(self.attachment(item, {'message_id':source['id'],'call_id':call_id}))
                    else:
                        raise ValueError('Unrecognized tool output item '+str(item.get('type')))
                if 'output' in state:
                    outputs.append(state['output'] if isinstance(state['output'],str) else dump(state['output']))
                if state.get('error') is not None:
                    error=state['error']
                    outputs.append(error if isinstance(error,str) else dump(error))
                if state.get('status') not in ('completed','error'):
                    outputs.append('[Historical OpenCode tool state: '+str(state.get('status'))+'; import does not execute or resume this tool]')
                tools.append((call, '\n\n'.join(outputs), p.get('time',{})))
            elif typ in ('step-start','step-finish','patch','file','compaction'):
                # Legacy metadata is retained in the immutable source snapshot.
                continue
            else:
                raise ValueError('Unrecognized assistant part '+str(typ))
        extra = {}
        if reasoning:
            extra['reasoning_content'] = '\n\n'.join(reasoning)
        if tools:
            extra['tool_calls'] = [call for call,_,_ in tools]
        if data.get('finish'):
            extra['finish_reason'] = data['finish']
        content = '\n\n'.join(texts)
        if data.get('error') and not content and not tools and not reasoning:
            content = '[Historical OpenCode assistant error]\n' + dump(data['error'])
        rows = [self.row('assistant',content,source,0,**extra)]
        for index,(call,result,times) in enumerate(tools,1):
            row=self.row('tool',result,source,index,tool_call_id=call['id'],tool_name=call['function']['name'])
            # Preserve original event ordering on the transcript; source tool times
            # are retained in source metadata, not used to reorder later user rows.
            rows.append(row)
        return rows

    def v2(self, source):
        data = json.loads(source['data'])
        typ = source['type']
        self.counts['v2:'+typ] += 1
        if typ == 'assistant':
            return self.assistant(source,data,data.get('content',[]))
        if typ == 'user':
            content = data.get('text','')
            attachments = [self.attachment(f, {'message_id':source['id'],'kind':'user'}) for f in data.get('files',[])]
            if attachments:
                content += '\n\n' + '\n\n'.join(attachments)
            return [self.row('user',content,source,0)]
        if typ == 'synthetic':
            return [self.row('user',data.get('text',''),source,0)]
        if typ == 'compaction' and data.get('status') == 'completed':
            text = '[Imported OpenCode context summary]\n' + data.get('summary','')
            if data.get('recent'):
                text += '\n\n' + data['recent']
            return [self.row('user',text,source,0)]
        # Old system instructions and control events remain visible historical
        # records, never instructions injected into a resumed Hermes model call.
        archived = {**source, '_active':False}
        text = data.get('text') if typ == 'system' else '[OpenCode '+typ+' event]\n'+dump(data)
        return [self.row('system',text or '',archived,0)]

    def legacy(self, source, parts):
        data = json.loads(source['data'])
        source = {**source, 'type':'legacy-'+data.get('role','unknown')}
        self.counts[source['type']] += 1
        if data.get('role') == 'assistant':
            return self.assistant(source,data,parts)
        if data.get('role') != 'user':
            raise ValueError('Unexpected legacy role')
        text = '\n\n'.join(p.get('text','') for p in parts if p.get('type') == 'text')
        attachments = [self.attachment(p, {'message_id':source['id'],'kind':'legacy-user'}) for p in parts if p.get('type') == 'file']
        if attachments:
            text += '\n\n' + '\n\n'.join(attachments)
        return [self.row('user',text,source,0)]
