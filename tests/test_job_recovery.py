from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.constants import (
    OPENAI_STATE_COMPLETED,
    OPENAI_STATE_SUBMITTED,
    POSTPROCESS_STATE_NOT_STARTED,
    POSTPROCESS_STATE_NOTIFY_PENDING,
)
from app.models import Job, JobEvent
from app.services import job as job_service
from app.services.job import claim_recovery_job_ids, recover_job

TestSessionFactory = async_sessionmaker[AsyncSession]


@pytest.fixture
def recovery_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """stale 판정과 재확인 간격을 고정해 시간 기반 테스트를 예측 가능하게 한다."""
    settings = SimpleNamespace(
        recovery_stale_after_seconds=60,
        recovery_poll_interval_seconds=30,
    )
    monkeypatch.setattr(job_service, "get_settings", lambda: settings)
    return settings


async def persist_recovery_job(
    session_factory: TestSessionFactory,
    job_factory: Callable[..., Job],
    **overrides: Any,
) -> int:
    """복구 시나리오에 필요한 Job을 저장하고 생성된 ID를 반환한다."""
    values: dict[str, Any] = {
        "external_request_id": "recovery-job-1",
        "openai_response_id": "response-1",
        "openai_state": OPENAI_STATE_SUBMITTED,
        "postprocess_state": POSTPROCESS_STATE_NOT_STARTED,
        "submitted_at": datetime.now(UTC) - timedelta(minutes=2),
    }
    values.update(overrides)

    async with session_factory() as session:
        async with session.begin():
            job = job_factory(**values)
            session.add(job)
            await session.flush()
            return job.id


async def load_job_with_events(
    session_factory: TestSessionFactory,
    job_id: int,
) -> tuple[Job, list[JobEvent]]:
    """복구 service가 commit한 Job과 이벤트를 새로운 session에서 조회한다."""
    async with session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        events = list(
            (
                await session.execute(
                    select(JobEvent)
                    .where(JobEvent.job_id == job_id)
                    .order_by(JobEvent.id)
                )
            )
            .scalars()
            .all()
        )
        return job, events


@pytest.mark.asyncio
async def test_claim_recovery_selects_only_stale_submitted_job(
    db_session_factory: TestSessionFactory,
    job_factory: Callable[..., Job],
    recovery_settings: SimpleNamespace,
):
    """복구 선점은 오래 멈춘 submitted Job만 선택한다."""
    stale_job_id = await persist_recovery_job(
        db_session_factory,
        job_factory,
        external_request_id="stale-job",
        openai_response_id="stale-response",
    )
    fresh_job_id = await persist_recovery_job(
        db_session_factory,
        job_factory,
        external_request_id="fresh-job",
        openai_response_id="fresh-response",
        submitted_at=datetime.now(UTC),
    )
    completed_job_id = await persist_recovery_job(
        db_session_factory,
        job_factory,
        external_request_id="completed-job",
        openai_response_id="completed-response",
        openai_state=OPENAI_STATE_COMPLETED,
    )

    # claim은 단순 조회가 아니라 선택한 Job의 last_recovery_at과 시도 횟수를
    # 같은 transaction에서 변경해 이번 복구 주기의 처리 대상을 표시한다.
    claimed_job_ids = await claim_recovery_job_ids(10)

    stale_job, _ = await load_job_with_events(db_session_factory, stale_job_id)
    fresh_job, _ = await load_job_with_events(db_session_factory, fresh_job_id)
    completed_job, _ = await load_job_with_events(
        db_session_factory,
        completed_job_id,
    )
    assert claimed_job_ids == [stale_job_id]
    assert stale_job.recovery_attempts == 1
    assert stale_job.last_recovery_at is not None
    assert fresh_job.recovery_attempts == 0
    assert fresh_job.last_recovery_at is None
    assert completed_job.recovery_attempts == 0


@pytest.mark.asyncio
async def test_recovery_marks_completed_response_for_notification(
    db_session_factory: TestSessionFactory,
    job_factory: Callable[..., Job],
    fake_openai_factory: Callable[..., Any],
    recovery_settings: SimpleNamespace,
):
    """stale 작업의 OpenAI 응답이 완료됐으면 결과 통지 대기 상태로 변경한다."""
    job_id = await persist_recovery_job(db_session_factory, job_factory)
    openai = fake_openai_factory(
        response={
            "id": "response-1",
            "status": "completed",
            "output": [{"type": "message", "content": "분석 완료"}],
        }
    )

    claimed_job_ids = await claim_recovery_job_ids(1)
    assert claimed_job_ids == [job_id]

    # fake client의 responses.retrieve()가 완료 상태를 반환해 실제 OpenAI
    # 네트워크 호출 없이 recover_job의 상태 반영 로직을 실행한다.
    await recover_job(job_id, openai_client=openai)

    job, events = await load_job_with_events(db_session_factory, job_id)
    assert openai.response_ids == ["response-1"]
    assert job.recovery_attempts == 1
    assert job.openai_state == OPENAI_STATE_COMPLETED
    assert job.postprocess_state == POSTPROCESS_STATE_NOTIFY_PENDING
    assert job.openai_response_payload is not None
    assert job.openai_response_payload["status"] == "completed"
    assert job.openai_terminal_at is not None
    assert job.next_retry_at is not None
    assert [event.event_type for event in events] == ["manual_recovery_marked"]
    assert events[0].source == "system"


@pytest.mark.asyncio
async def test_recovery_retrieve_failure_keeps_submitted_state(
    db_session_factory: TestSessionFactory,
    job_factory: Callable[..., Job],
    fake_openai_factory: Callable[..., Any],
    recovery_settings: SimpleNamespace,
):
    """OpenAI 상태 조회가 실패하면 submitted 상태를 유지하고 확인 이벤트를 남긴다."""
    job_id = await persist_recovery_job(db_session_factory, job_factory)
    openai = fake_openai_factory(error=TimeoutError("OpenAI retrieve timeout"))

    claimed_job_ids = await claim_recovery_job_ids(1)
    assert claimed_job_ids == [job_id]

    await recover_job(job_id, openai_client=openai)

    job, events = await load_job_with_events(db_session_factory, job_id)
    assert openai.response_ids == ["response-1"]
    assert job.recovery_attempts == 1
    assert job.openai_state == OPENAI_STATE_SUBMITTED
    assert job.postprocess_state == POSTPROCESS_STATE_NOT_STARTED
    assert job.openai_response_payload is None
    assert [event.event_type for event in events] == ["recovery_checked"]
    assert events[0].payload == {"error": "OpenAI retrieve timeout"}
