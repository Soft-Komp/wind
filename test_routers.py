import sys
sys.path.insert(0, '/app')
from fastapi import FastAPI

routers = [
    'auth', 'users', 'roles', 'roles_permissions', 'permissions',
    'debtors', 'monits', 'snapshots', 'events', 'system', 'templates',
    'faktury_akceptacja', 'moje_faktury',
]

for name in routers:
    try:
        mod = __import__(f'app.api.{name}', fromlist=[name])
        app = FastAPI()
        for attr in ['router', 'roles_router', 'api_router']:
            r = getattr(mod, attr, None)
            if r:
                app.include_router(r)
        app.openapi()
        print(f'OK     {name}')
    except Exception as e:
        print(f'FAIL   {name}: {e}')
