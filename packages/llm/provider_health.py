"""Non-secret API availability receipts, independent of a process heartbeat."""
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import tempfile


def _path():
    return Path(os.environ.get('LLM_PROVIDER_HEALTH_PATH') or str(Path.home()/'.local/state/tenant-issue-os/llm-provider-health.json'))


def record(code=None):
    now = datetime.now(timezone.utc).isoformat()
    cause = ('credit_exhausted' if code in {'insufficient_quota','credit_balance_exhausted'}
             else 'authentication' if code in {'invalid_api_key','authentication_error'}
             else 'api_error' if code else None)
    payload = {'state':'blocked' if cause else 'ready','has_error':bool(cause),'cause':cause,'checked_at':now}
    try:
        path=_path();path.parent.mkdir(parents=True,exist_ok=True)
        fd,name=tempfile.mkstemp(dir=path.parent,prefix='.llm-health-')
        try:
            with os.fdopen(fd,'w') as h:json.dump(payload,h)
            os.replace(name,path)
        finally:
            if os.path.exists(name):os.unlink(name)
    except OSError:
        pass  # Health storage must not turn a successful API call into a failure.


def public_status():
    try:
        data=json.loads(_path().read_text())
        return {k:data.get(k) for k in ('state','has_error','cause','checked_at')}
    except (OSError,ValueError,TypeError):
        return {'state':'unverified','has_error':False,'cause':None}


def deferred_error():
    """A five-minute breaker prevents a request storm while funds are absent."""
    state=public_status()
    if state.get('state')!='blocked' or state.get('cause') not in {'credit_exhausted','authentication'}:
        return None
    try:age=(datetime.now(timezone.utc)-datetime.fromisoformat(state['checked_at'])).total_seconds()
    except (ValueError,TypeError,KeyError):return None
    if 0<=age<300:
        return 'insufficient_quota' if state['cause']=='credit_exhausted' else 'invalid_api_key'
    return None
