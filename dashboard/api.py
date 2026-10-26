"""Private same-profile owner API; never a model-facing tool or a second writer."""
import asyncio
import importlib
import importlib.util
from pathlib import Path
import sys

from fastapi import APIRouter, HTTPException, Request

PACKAGE='hermes_local_opencode_sync'
root=Path(__file__).resolve().parents[1]
if PACKAGE not in sys.modules:
    spec=importlib.util.spec_from_file_location(PACKAGE,root/'__init__.py',submodule_search_locations=[str(root)])
    module=importlib.util.module_from_spec(spec)
    sys.modules[PACKAGE]=module
    spec.loader.exec_module(module)
engine=importlib.import_module(PACKAGE+'.engine')
router=APIRouter()


def owner(request):
    if getattr(request.state,'native_http_principal',None) is None:
        raise HTTPException(status_code=401,detail='A private local owner ticket is required')
    from hermes_cli.web_server_sessions import _session_mutation_context
    authority,actor=_session_mutation_context(request,None)
    if authority is None or not {'session:create','session:control'}<=actor.capabilities:
        raise HTTPException(status_code=503,detail='This plugin requires the profile gateway owner in plugins.isolation: in_process mode')
    if authority.profile_id!=actor.profile_id:
        raise HTTPException(status_code=403,detail='profile_mismatch')
    return authority,actor


@router.get('/health')
async def health(request:Request):
    authority,_=owner(request)
    return {'ok':True,'version':1,'profile_id':authority.profile_id}


@router.post('/segment')
async def segment(request:Request):
    authority,actor=owner(request)
    raw=bytearray()
    async for chunk in request.stream():
        if len(raw)+len(chunk)>8*1024*1024:
            raise HTTPException(status_code=413,detail='Sync segment exceeds the request budget')
        raw.extend(chunk)
    try:
        import json
        body=json.loads(raw)
        engine.validate_body(authority.db,body)
    except (ValueError,TypeError,KeyError) as exc:
        raise HTTPException(status_code=400,detail=str(exc)) from exc
    from gateway.session_runtime_workers import track_mutation
    from hermes_state_mutation_guards import MUTATION_GUARD_REFUSALS
    from hermes_state_runtime import RuntimeStoreError
    backup_root=Path(authority.profile_id)/'imports'/'opencode-sync'/'backups'
    try:
        task=track_mutation(authority,engine.apply_owned(authority,actor,body,backup_root))
        return await asyncio.shield(task)
    except engine.SyncRefusal as exc:
        raise HTTPException(status_code=409,detail=str(exc)) from exc
    except (RuntimeStoreError,*MUTATION_GUARD_REFUSALS) as exc:
        raise HTTPException(status_code=409,detail=getattr(exc,'reason','session_busy')) from exc
