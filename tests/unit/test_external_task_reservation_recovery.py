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
