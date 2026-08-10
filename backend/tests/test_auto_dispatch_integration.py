from __future__ import annotations

import importlib.util
import sys
import types
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
AUTO_PATH = ROOT / "worker/tasks/auto_dispatch_task.py"
FILTER_ENGINE_PATH = ROOT / "worker/services/filter_engine.py"


class AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, exc_type, exc, tb):
        return False


class UpdateResult:
    rowcount = 1


class FakeConnection:
    def __init__(self):
        self.executions = []

    async def execute(self, statement, params=None):
        self.executions.append((str(statement), params))
        return UpdateResult()


class FakeEngine:
    def __init__(self):
        self.connection = FakeConnection()

    def connect(self):
        return AsyncContext(self.connection)

    def begin(self):
        return AsyncContext(self.connection)


class EventLog:
    def __init__(self):
        self.events = []

    def log(self, name, payload):
        self.events.append((name, payload))


def load_auto_dispatch(monkeypatch):
    worker = types.ModuleType("worker")
    worker.__path__ = [str(ROOT / "worker")]
    monkeypatch.setitem(sys.modules, "worker", worker)

    core = types.ModuleType("worker.core")
    core.__path__ = []
    monkeypatch.setitem(sys.modules, "worker.core", core)

    db_mod = types.ModuleType("worker.core.db")
    db_mod.get_engine = lambda: None
    monkeypatch.setitem(sys.modules, "worker.core.db", db_mod)

    logging_mod = types.ModuleType("worker.core.logging_setup")
    logging_mod.get_event_logger = lambda _path: EventLog()
    monkeypatch.setitem(sys.modules, "worker.core.logging_setup", logging_mod)

    settings_mod = types.ModuleType("worker.settings")
    settings_mod.get_settings = lambda: types.SimpleNamespace(LOG_DIR="/tmp")
    monkeypatch.setitem(sys.modules, "worker.settings", settings_mod)

    services = types.ModuleType("worker.services")
    services.__path__ = [str(ROOT / "worker/services")]
    monkeypatch.setitem(sys.modules, "worker.services", services)

    spec_engine = importlib.util.spec_from_file_location(
        "worker.services.filter_engine", FILTER_ENGINE_PATH
    )
    engine_module = importlib.util.module_from_spec(spec_engine)
    assert spec_engine.loader is not None
    spec_engine.loader.exec_module(engine_module)
    monkeypatch.setitem(sys.modules, "worker.services.filter_engine", engine_module)

    spec = importlib.util.spec_from_file_location("tested_auto_dispatch", AUTO_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def auto_dispatch(monkeypatch):
    return load_auto_dispatch(monkeypatch)


def test_build_filter_document_reconstructs_unified_fields(auto_dispatch):
    doc = {
        "id_instance": 1,
        "id_source": 4,
        "id_document": "KSEF-1",
        "source_name": "ksef_fakir",
        "document_title": "FV/1/2026",
        "document_amount": 123.45,
        "extra_data": {
            "doc_number": "FV/1/2026",
            "doc_date": "2026-08-01",
            "contractor": "Firma A",
            "nip": "1234567890",
            "currency": "PLN",
            "amount_gross": 123.45,
            "custom_flag": "X",
        },
    }

    result = auto_dispatch._build_filter_document(doc)

    assert result["source_name"] == "ksef_fakir"
    assert result["doc_number"] == "FV/1/2026"
    assert result["amount_gross"] == Decimal("123.45")
    assert result["contractor_name"] == "Firma A"
    assert result["extra"]["custom_flag"] == "X"
    assert result["extra_data"]["custom_flag"] == "X"


def test_parse_extra_data_accepts_json_string_and_dict(auto_dispatch):
    assert auto_dispatch._parse_extra_data('{"a": 1}') == {"a": 1}
    original = {"b": 2}
    result = auto_dispatch._parse_extra_data(original)
    assert result == original
    assert result is not original


@pytest.mark.asyncio
async def test_dispatch_calls_worker_engine_with_correct_argument_order(auto_dispatch, monkeypatch):
    fake_engine = FakeEngine()
    monkeypatch.setattr(auto_dispatch, "get_engine", lambda: fake_engine)
    captured = {}

    async def fake_resolve(conn, id_source, doc_data):
        captured["conn"] = conn
        captured["id_source"] = id_source
        captured["doc_data"] = doc_data
        return 33

    monkeypatch.setattr(auto_dispatch, "resolve_path", fake_resolve)
    event_log = EventLog()
    result = await auto_dispatch._dispatch_one(
        {
            "id_instance": 10,
            "id_source": 4,
            "id_document": "D10",
            "document_amount": 500,
            "document_title": "FV/10",
            "extra_data": '{"currency":"PLN"}',
            "dispatch_attempts": 0,
            "source_name": "fakir",
        },
        5,
        None,
        event_log,
    )

    assert result == "dispatched"
    assert captured["conn"] is fake_engine.connection
    assert captured["id_source"] == 4
    assert captured["doc_data"]["id_document"] == "D10"
    assert event_log.events[-1][0] == "auto_dispatched"


@pytest.mark.asyncio
async def test_filter_engine_error_does_not_increment_attempts(auto_dispatch, monkeypatch):
    fake_engine = FakeEngine()
    monkeypatch.setattr(auto_dispatch, "get_engine", lambda: fake_engine)

    async def failing_resolve(conn, id_source, doc_data):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(auto_dispatch, "resolve_path", failing_resolve)
    event_log = EventLog()
    result = await auto_dispatch._dispatch_one(
        {
            "id_instance": 10,
            "id_source": 4,
            "id_document": "D10",
            "document_amount": 500,
            "document_title": "FV/10",
            "extra_data": "{}",
            "dispatch_attempts": 4,
            "source_name": "fakir",
        },
        5,
        None,
        event_log,
    )

    assert result == "skipped"
    assert fake_engine.connection.executions == []
    assert event_log.events[-1][0] == "auto_dispatch_filter_engine_error"

def test_auto_dispatch_has_no_backend_import_or_unsafe_fallback():
    source = AUTO_PATH.read_text(encoding="utf-8")
    assert "from app.services.filter_engine" not in source
    assert "_simple_path_lookup" not in source
    assert "from worker.services.filter_engine import resolve_path" in source
