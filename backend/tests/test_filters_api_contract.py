from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
FILTERS_PATH = ROOT / "backend/app/api/approval/filters.py"


class DummyRouter:
    def __init__(self, *args, **kwargs):
        pass

    def get(self, *args, **kwargs):
        return lambda fn: fn

    def post(self, *args, **kwargs):
        return lambda fn: fn

    def patch(self, *args, **kwargs):
        return lambda fn: fn

    def delete(self, *args, **kwargs):
        return lambda fn: fn


class DummyHTTPException(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class FakeAuditService:
    def __init__(self):
        self.calls = []

    def log_crud(self, db, **kwargs):
        self.calls.append(kwargs)
        return None


class FakeResult:
    def __init__(self, row=None, rowcount=1):
        self._row = row
        self.rowcount = rowcount

    def fetchone(self):
        return self._row


class FakeDB:
    def __init__(self, results):
        self.results = list(results)
        self.commits = 0

    async def execute(self, _statement, _params=None):
        if not self.results:
            raise AssertionError("Brak wyniku FakeDB")
        return self.results.pop(0)

    async def commit(self):
        self.commits += 1

    async def rollback(self):
        pass


def install_stubs(monkeypatch):
    fastapi = types.ModuleType("fastapi")
    fastapi.APIRouter = DummyRouter
    fastapi.HTTPException = DummyHTTPException
    fastapi.Request = type("Request", (), {})
    fastapi.status = types.SimpleNamespace(
        HTTP_200_OK=200,
        HTTP_201_CREATED=201,
        HTTP_202_ACCEPTED=202,
    )
    monkeypatch.setitem(sys.modules, "fastapi", fastapi)

    app = types.ModuleType("app")
    app.__path__ = []
    monkeypatch.setitem(sys.modules, "app", app)

    schemas = types.ModuleType("app.schemas")
    schemas.__path__ = []
    common = types.ModuleType("app.schemas.common")
    common.dt_utc = lambda value: value.isoformat() if value else None
    monkeypatch.setitem(sys.modules, "app.schemas", schemas)
    monkeypatch.setitem(sys.modules, "app.schemas.common", common)

    core = types.ModuleType("app.core")
    core.__path__ = []
    deps = types.ModuleType("app.core.dependencies")
    deps.DB = Any
    deps.CurrentUser = Any
    deps.RedisClient = Any
    deps.require_permission = lambda _permission: (lambda: None)
    monkeypatch.setitem(sys.modules, "app.core", core)
    monkeypatch.setitem(sys.modules, "app.core.dependencies", deps)

    api = types.ModuleType("app.api")
    api.__path__ = []
    approval_api = types.ModuleType("app.api.approval")
    approval_api.__path__ = []
    delete_helpers = types.ModuleType("app.api.approval._delete_helpers")

    async def generate_delete_token(*args, **kwargs):
        return "token", 60

    async def verify_delete_token(*args, **kwargs):
        return None

    delete_helpers.generate_delete_token = generate_delete_token
    delete_helpers.verify_delete_token = verify_delete_token
    monkeypatch.setitem(sys.modules, "app.api", api)
    monkeypatch.setitem(sys.modules, "app.api.approval", approval_api)
    monkeypatch.setitem(sys.modules, "app.api.approval._delete_helpers", delete_helpers)

    services = types.ModuleType("app.services")
    services.__path__ = []
    audit = FakeAuditService()
    services.audit_service = audit
    approval_service = types.ModuleType("app.services.approval_service")

    async def _check_module_enabled(*args, **kwargs):
        return None

    approval_service._check_module_enabled = _check_module_enabled
    monkeypatch.setitem(sys.modules, "app.services", services)
    monkeypatch.setitem(sys.modules, "app.services.approval_service", approval_service)
    return audit


@pytest.fixture
def filters_module(monkeypatch):
    audit = install_stubs(monkeypatch)
    spec = importlib.util.spec_from_file_location("tested_filters_api", FILTERS_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    module._test_audit = audit
    return module


def test_create_schema_defaults_to_and_and_normalizes_case(filters_module):
    base = {
        "filter_name": "Kwoty",
        "filter_type": "standard",
        "id_path": 1,
    }
    assert filters_module.FilterCreateBody(**base).logic_operator == "AND"
    assert filters_module.FilterCreateBody(**base, logic_operator=" or ").logic_operator == "OR"


def test_create_schema_rejects_invalid_operator(filters_module):
    with pytest.raises(ValidationError):
        filters_module.FilterCreateBody(
            filter_name="Kwoty",
            filter_type="standard",
            id_path=1,
            logic_operator="XOR",
        )


def test_patch_schema_normalizes_operator(filters_module):
    assert filters_module.FilterPatchBody(logic_operator="and").logic_operator == "AND"


@pytest.mark.asyncio
async def test_create_filter_persists_and_audits_operator(filters_module):
    now = datetime(2026, 8, 3, 8, 0, 0)
    created_row = (7, "Kwoty", "standard", 2, 4, 100, 1, None, "OR", now, now)
    db = FakeDB([
        FakeResult(row=(7,)),
        FakeResult(row=created_row),
    ])
    user = types.SimpleNamespace(id_user=9, username="admin")
    body = filters_module.FilterCreateBody(
        filter_name="Kwoty",
        filter_type="standard",
        id_path=2,
        id_source=4,
        logic_operator="OR",
    )

    result = await filters_module.create_filter(body, user, db, None)

    assert result["logic_operator"] == "OR"
    assert db.commits == 1
    assert filters_module._test_audit.calls[-1]["action"] == "approval_filter_created"
    assert filters_module._test_audit.calls[-1]["new_value"]["logic_operator"] == "OR"


@pytest.mark.asyncio
async def test_update_filter_audits_old_and_new_operator(filters_module):
    now = datetime(2026, 8, 3, 8, 0, 0)
    old_row = (7, "Kwoty", "standard", 2, 4, 100, 1, None, "AND", now, now)
    new_row = (7, "Kwoty", "standard", 2, 4, 100, 1, None, "OR", now, now)
    db = FakeDB([
        FakeResult(row=old_row),
        FakeResult(rowcount=1),
        FakeResult(row=new_row),
    ])
    user = types.SimpleNamespace(id_user=9, username="admin")

    result = await filters_module.update_filter(
        7,
        filters_module.FilterPatchBody(logic_operator="OR"),
        user,
        db,
        None,
    )

    assert result["logic_operator"] == "OR"
    audit = filters_module._test_audit.calls[-1]
    assert audit["action"] == "approval_filter_updated"
    assert audit["old_value"]["logic_operator"] == "AND"
    assert audit["new_value"]["logic_operator"] == "OR"
    assert audit["details"]["logic_operator_changed"] is True

@pytest.mark.asyncio
async def test_universal_filter_rejects_or_operator(filters_module):
    db = FakeDB([])
    user = types.SimpleNamespace(id_user=9, username="admin")
    body = filters_module.FilterCreateBody(
        filter_name="Funkcja SQL",
        filter_type="universal",
        id_path=2,
        universal_function="dbo_test",
        logic_operator="OR",
    )

    with pytest.raises(DummyHTTPException) as exc:
        await filters_module.create_filter(body, user, db, None)

    assert exc.value.status_code == 422
    assert "standard" in exc.value.detail
