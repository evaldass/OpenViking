"""Recovered Work admission must not grant unsubmitted tasks target ownership."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from openviking.service.external_task_service import ExternalTaskService
from openviking.service.task_tracker import TaskRecord, TaskStatus


@pytest.mark.parametrize(
    "status", [TaskStatus.RUNNING, TaskStatus.CANCELLING, TaskStatus.CANCELLED]
)
@pytest.mark.parametrize("stage", [None, "queued", "submitting", "polling"])
@pytest.mark.parametrize("submitted", [False, True])
async def test_restore_reserves_submission_intent_or_runtime_id_only(status, stage, submitted):
    service = ExternalTaskService()
    service.register(SimpleNamespace(task_type="compile", serialization_key=lambda p: p["to"]))
    task = TaskRecord(
        task_id="task",
        task_type="compile",
        account_id="account",
        user_id="alice",
        status=status,
        stage=stage,
        meta={"request": {"to": "viking://resources/wiki"}},
    )
    service._task_auth = AsyncMock(
        return_value={"external_task_id": "runtime"} if submitted else {}
    )
    await service.restore_tasks([task])
    expected = submitted or stage in {"submitting", "polling"}
    assert bool(service._owners) is expected
    if expected:
        assert service._owners == {("account", "compile", "viking://resources/wiki"): {"task"}}


@pytest.mark.parametrize("outcome", ["result", "error"])
@pytest.mark.parametrize("submitted", [False, True])
async def test_cancelled_restore_retains_runtime_despite_persisted_outcome(outcome, submitted):
    service = ExternalTaskService()
    service.register(SimpleNamespace(task_type="compile", serialization_key=lambda p: p["to"]))
    task = TaskRecord(
        task_id="cancelled",
        task_type="compile",
        account_id="account",
        user_id="alice",
        status=TaskStatus.CANCELLED,
        stage="queued",
        meta={"request": {"to": "viking://resources/wiki"}},
        **{outcome: {"message": "cancelled"}},
    )
    service._task_auth = AsyncMock(
        return_value={"external_task_id": "runtime"} if submitted else {}
    )
    await service.restore_tasks([task])
    assert bool(service._owners) is submitted
    if submitted:
        assert service._owners == {("account", "compile", "viking://resources/wiki"): {"cancelled"}}


@pytest.mark.parametrize("auth", [None, [], "invalid"])
async def test_restore_malformed_durable_auth_never_creates_phantom_owner(auth):
    service = ExternalTaskService()
    service.register(SimpleNamespace(task_type="compile", serialization_key=lambda p: p["to"]))
    task = TaskRecord(
        task_id="waiting",
        task_type="compile",
        account_id="account",
        user_id="alice",
        status=TaskStatus.RUNNING,
        stage="queued",
        meta={"request": {"to": "viking://resources/wiki"}},
    )
    service._task_auth = AsyncMock(return_value=auth)
    with pytest.raises(AttributeError):
        await service.restore_tasks([task])
    assert service._owners == {}


async def test_restore_durable_auth_failure_never_creates_phantom_owner():
    service = ExternalTaskService()
    service.register(SimpleNamespace(task_type="compile", serialization_key=lambda p: p["to"]))
    task = TaskRecord(
        task_id="waiting",
        task_type="compile",
        account_id="account",
        user_id="alice",
        status=TaskStatus.RUNNING,
        stage="queued",
        meta={"request": {"to": "viking://resources/wiki"}},
    )
    service._task_auth = AsyncMock(side_effect=OSError("durable auth unavailable"))
    with pytest.raises(OSError, match="durable auth unavailable"):
        await service.restore_tasks([task])
    assert service._owners == {}
