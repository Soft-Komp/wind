from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATHS = [
    ROOT / "backend/app/services/filter_engine.py",
    ROOT / "worker/services/filter_engine.py",
]


class FakeResult:
    def __init__(self, rows=None, scalar_value=None):
        self._rows = list(rows or [])
        self._scalar_value = scalar_value

    def fetchall(self):
        return list(self._rows)

    def scalar(self):
        return self._scalar_value


class FakeDB:
    def __init__(self, results):
        self._results = list(results)

    async def execute(self, _statement, _params=None):
        if not self._results:
            raise AssertionError("Brak przygotowanego wyniku FakeDB")
        return self._results.pop(0)


def load_module(path: Path):
    name = "tested_" + "_".join(path.parts[-4:]).replace(".", "_")
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(params=MODULE_PATHS, ids=["backend", "worker"])
def filter_engine(request):
    return load_module(request.param)


@pytest.mark.asyncio
async def test_and_requires_all_conditions(filter_engine):
    db = FakeDB([
        FakeResult(rows=[
            ("amount_gross", "gte", "100"),
            ("currency", "eq", "PLN"),
        ])
    ])
    assert await filter_engine._evaluate_standard_filter(
        db,
        10,
        {"amount_gross": 150, "currency": "EUR"},
        "AND",
    ) is False


@pytest.mark.asyncio
async def test_or_requires_at_least_one_condition(filter_engine):
    db = FakeDB([
        FakeResult(rows=[
            ("amount_gross", "gte", "100"),
            ("currency", "eq", "PLN"),
        ])
    ])
    assert await filter_engine._evaluate_standard_filter(
        db,
        11,
        {"amount_gross": 150, "currency": "EUR"},
        "OR",
    ) is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("logic_operator", "expected"),
    [("AND", True), ("OR", False)],
)
async def test_empty_filter_semantics(filter_engine, logic_operator, expected):
    db = FakeDB([FakeResult(rows=[])])
    assert await filter_engine._evaluate_standard_filter(
        db,
        12,
        {},
        logic_operator,
    ) is expected


@pytest.mark.asyncio
async def test_single_condition_same_result_for_and_or(filter_engine):
    for logic_operator in ("AND", "OR"):
        db = FakeDB([FakeResult(rows=[("nip", "eq", "123")])])
        assert await filter_engine._evaluate_standard_filter(
            db,
            13,
            {"nip": "123"},
            logic_operator,
        ) is True


@pytest.mark.asyncio
async def test_invalid_logic_operator_falls_back_to_and(filter_engine):
    db = FakeDB([
        FakeResult(rows=[
            ("amount_gross", "gte", "100"),
            ("currency", "eq", "PLN"),
        ])
    ])
    assert await filter_engine._evaluate_standard_filter(
        db,
        14,
        {"amount_gross": 150, "currency": "EUR"},
        "XOR",
    ) is False


@pytest.mark.asyncio
async def test_last_matching_filter_with_highest_priority_wins(filter_engine):
    db = FakeDB([
        FakeResult(rows=[
            (1, "standard", 100, None, 10, "AND"),
            (2, "standard", 200, None, 20, "OR"),
        ]),
        FakeResult(rows=[]),
        FakeResult(rows=[("currency", "eq", "PLN")]),
    ])
    result = await filter_engine.resolve_path(
        db,
        5,
        {"id_document": "D1", "id_source": 5, "currency": "PLN"},
    )
    assert result == 200


def test_backend_and_worker_public_signature_are_equal():
    backend = load_module(MODULE_PATHS[0])
    worker = load_module(MODULE_PATHS[1])
    import inspect

    backend_sig = inspect.signature(backend.resolve_path)
    worker_sig = inspect.signature(worker.resolve_path)
    assert [(p.name, p.kind, p.default) for p in backend_sig.parameters.values()] == [
        (p.name, p.kind, p.default) for p in worker_sig.parameters.values()
    ]
    assert backend._VALID_LOGIC_OPERATORS == worker._VALID_LOGIC_OPERATORS
    assert backend._DEFAULT_LOGIC_OPERATOR == worker._DEFAULT_LOGIC_OPERATOR == "AND"
