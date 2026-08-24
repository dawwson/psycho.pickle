import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.constants import (
    OPENAI_STATE_COMPLETED,
    POSTPROCESS_STATE_NOTIFY_FAILED,
    POSTPROCESS_STATE_NOTIFY_IN_PROGRESS,
    POSTPROCESS_STATE_NOTIFY_PENDING,
    POSTPROCESS_STATE_NOTIFY_SUCCEEDED,
)
from app.models import Job, JobEvent
from app.services import job as job_service
from app.services.job import claim_due_postprocess_work, process_postprocess_work

TestSessionFactory = async_sessionmaker[AsyncSession]


@pytest.fixture
def notify_settings(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """통지 테스트가 실제 환경변수 대신 고정된 SQS 설정을 사용하게 한다."""
    settings = SimpleNamespace(
        business_notify_sqs_queue_url="https://sqs.example.com/pickle-result",
        business_notify_url=None,
        business_notify_sqs_message_group_id=None,
        business_notify_timeout_seconds=10.0,
        business_notify_auth_header_name=None,
        business_notify_auth_header_value=None,
        lease_seconds=300,
    )
    # get_settings()는 service 내부에서 호출되므로 monkeypatch로 반환값을
    # 고정하면 테스트가 로컬 .env나 운영 queue URL에 의존하지 않는다.
    monkeypatch.setattr(job_service, "get_settings", lambda: settings)
    return settings


async def persist_notify_pending_job(
    session_factory: TestSessionFactory,
    job_factory: Callable[..., Job],
) -> int:
    """통지 직전 상태의 Job을 저장하고 생성된 ID를 반환한다."""
    async with session_factory() as session:
        async with session.begin():
            job = job_factory(
                openai_response_id="langchain-response-1",
                openai_state=OPENAI_STATE_COMPLETED,
                postprocess_state=POSTPROCESS_STATE_NOTIFY_PENDING,
                result_payload={"analysis": "스프린트 분석 완료"},
                openai_terminal_at=datetime.now(UTC),
                next_retry_at=datetime.now(UTC),
            )
            session.add(job)
            # flush()는 transaction을 commit하지 않고 INSERT를 먼저 실행해
            # DB가 생성한 job.id를 같은 transaction 안에서 사용할 수 있게 한다.
            await session.flush()
            return job.id


async def load_job_with_events(
    session_factory: TestSessionFactory,
    job_id: int,
) -> tuple[Job, list[JobEvent]]:
    """통지 service가 commit한 Job과 이벤트를 새로운 session에서 조회한다."""
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
async def test_sqs_notification_success_marks_job_succeeded(
    db_session_factory: TestSessionFactory,
    job_factory: Callable[..., Job],
    fake_sqs_factory: Callable[..., Any],
    notify_settings: SimpleNamespace,
):
    """SQS 결과 통지가 성공하면 Job을 통지 완료 상태로 변경한다."""
    job_id = await persist_notify_pending_job(db_session_factory, job_factory)
    sqs = fake_sqs_factory()

    # process_postprocess_work는 worker가 work kind에 따라 실제 통지 함수를
    # 선택하는 공개 진입점이다. fake client를 넘겨 실제 AWS 호출을 막는다.
    await process_postprocess_work("notify", job_id, sqs_client=sqs)

    job, events = await load_job_with_events(db_session_factory, job_id)
    request_body = json.loads(sqs.requests[0]["MessageBody"])
    assert len(sqs.requests) == 1
    assert sqs.requests[0]["QueueUrl"] == (
        notify_settings.business_notify_sqs_queue_url
    )
    assert request_body["external_request_id"] == "analysis-request-1"
    assert request_body["result"] == {"analysis": "스프린트 분석 완료"}
    assert job.postprocess_state == POSTPROCESS_STATE_NOTIFY_SUCCEEDED
    assert job.notify_attempts == 1
    assert job.notified_at is not None
    assert job.postprocess_error is None
    assert job.next_retry_at is None
    assert [event.event_type for event in events] == [
        "notify_attempt",
        "notify_succeeded",
    ]


@pytest.mark.asyncio
async def test_sqs_notification_failure_schedules_retry(
    db_session_factory: TestSessionFactory,
    job_factory: Callable[..., Job],
    fake_sqs_factory: Callable[..., Any],
    notify_settings: SimpleNamespace,
):
    """SQS 결과 통지가 실패하면 오류를 저장하고 다음 재시도를 예약한다."""
    job_id = await persist_notify_pending_job(db_session_factory, job_factory)
    sqs = fake_sqs_factory(error=TimeoutError("SQS timeout"))
    before_attempt = datetime.now(UTC)

    await process_postprocess_work("notify", job_id, sqs_client=sqs)

    job, events = await load_job_with_events(db_session_factory, job_id)
    assert len(sqs.requests) == 1
    assert job.postprocess_state == POSTPROCESS_STATE_NOTIFY_FAILED
    assert job.notify_attempts == 1
    assert job.postprocess_error == "SQS timeout"
    assert job.postprocess_error_payload is not None
    assert job.next_retry_at is not None
    # SQLite는 timezone 정보가 있는 datetime도 조회할 때 naive datetime으로
    # 반환하므로 같은 형태로 맞춘 뒤 미래 시각인지 비교한다.
    assert job.next_retry_at > before_attempt.replace(tzinfo=None)
    assert job.notified_at is None
    assert [event.event_type for event in events] == [
        "notify_attempt",
        "notify_failed",
    ]


@pytest.mark.asyncio
async def test_failed_notification_is_claimed_and_succeeds_on_retry(
    db_session_factory: TestSessionFactory,
    job_factory: Callable[..., Job],
    fake_sqs_factory: Callable[..., Any],
    notify_settings: SimpleNamespace,
):
    """재시도 시각이 지난 통지 실패 Job을 선점해 다시 성공 처리한다."""
    job_id = await persist_notify_pending_job(db_session_factory, job_factory)
    failed_sqs = fake_sqs_factory(error=TimeoutError("temporary SQS failure"))
    recovered_sqs = fake_sqs_factory()

    await process_postprocess_work("notify", job_id, sqs_client=failed_sqs)

    # 실패 직후 next_retry_at은 미래이므로 아직 claim되지 않는다. 테스트에서는
    # 시간이 지난 상황을 만들기 위해 DB 시각만 과거로 이동한다.
    async with db_session_factory() as session:
        async with session.begin():
            job = await session.get(Job, job_id)
            assert job is not None
            job.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)

    # claim은 실행 가능한 Job을 찾아 notify_in_progress로 바꾸고 lease를 잡는다.
    # worker는 반환된 (work kind, job ID) 목록을 실제 처리 함수에 전달한다.
    work_items = await claim_due_postprocess_work(1)

    claimed_job, _ = await load_job_with_events(db_session_factory, job_id)
    assert work_items == [("notify", job_id)]
    assert claimed_job.postprocess_state == POSTPROCESS_STATE_NOTIFY_IN_PROGRESS

    await process_postprocess_work("notify", job_id, sqs_client=recovered_sqs)

    job, events = await load_job_with_events(db_session_factory, job_id)
    assert len(failed_sqs.requests) == 1
    assert len(recovered_sqs.requests) == 1
    assert job.postprocess_state == POSTPROCESS_STATE_NOTIFY_SUCCEEDED
    assert job.notify_attempts == 2
    assert job.postprocess_error is None
    assert job.postprocess_error_payload is None
    assert job.next_retry_at is None
    assert [event.event_type for event in events] == [
        "notify_attempt",
        "notify_failed",
        "notify_attempt",
        "notify_succeeded",
    ]
