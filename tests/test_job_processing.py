from collections.abc import Callable
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.constants import (
    OPENAI_STATE_COMPLETED,
    OPENAI_STATE_PENDING,
    POSTPROCESS_STATE_NOT_STARTED,
    POSTPROCESS_STATE_NOTIFY_PENDING,
)
from app.models import Job, JobEvent
from app.services.job import process_sqs_message

TestSessionFactory = async_sessionmaker[AsyncSession]
# 긴 generic type을 별칭으로 만들어 fixture와 helper의 타입을 짧게 표현한다.


async def load_job_with_events(
    session_factory: TestSessionFactory,
    external_request_id: str,
) -> tuple[Job, list[JobEvent]]:
    """service가 commit한 상태를 새 session에서 다시 읽는다."""
    async with session_factory() as session:
        # execute()는 SQLAlchemy query를 실행하고 scalar_one()은 결과가 정확히
        # 한 행이어야 한다는 전제까지 검증하면서 Job 객체를 꺼낸다.
        job = (
            await session.execute(
                select(Job).where(Job.external_request_id == external_request_id)
            )
        ).scalar_one()
        events = list(
            (
                await session.execute(
                    select(JobEvent)
                    .where(JobEvent.job_id == job.id)
                    .order_by(JobEvent.id)
                )
            )
            .scalars()
            .all()
        )
        # tuple로 반환하면 호출한 테스트가 Job 상태와 관련 이벤트를 함께 검증한다.
        return job, events


@pytest.mark.asyncio
async def test_new_sqs_job_completes_and_records_events(
    db_session_factory: TestSessionFactory,
    sqs_message_factory: Callable[..., dict[str, Any]],
    fake_llm_factory: Callable[..., Any],
):
    """새 SQS 작업이 성공하면 결과와 완료 이벤트를 저장한다."""
    # pytest.mark.asyncio가 async test를 event loop에서 실행하므로 service의
    # coroutine을 일반 pytest test 안에서 await할 수 있다.
    # fixture 이름을 테스트 매개변수로 선언하면 pytest가 conftest.py에서 찾아
    # 생성한 값을 주입한다. 이 테스트에서는 실제 DB session과 fake LLM을 조합한다.
    message = sqs_message_factory()
    llm = fake_llm_factory(content="스프린트 분석 완료")

    should_delete = await process_sqs_message(message, langchain_client=llm)

    job, events = await load_job_with_events(
        db_session_factory,
        "analysis-request-1",
    )
    # 반환값과 외부 호출 횟수는 worker가 메시지를 삭제해도 되는지와 실제 LLM
    # 호출 여부를, 아래 DB assertion은 commit된 lifecycle 상태를 검증한다.
    assert should_delete is True
    assert len(llm.calls) == 1
    assert job.openai_state == OPENAI_STATE_COMPLETED
    assert job.postprocess_state == POSTPROCESS_STATE_NOTIFY_PENDING
    assert job.submission_attempts == 1
    assert job.result_payload == {"analysis": "스프린트 분석 완료"}
    assert job.openai_error is None
    assert [event.event_type for event in events] == [
        "sqs_received",
        "langchain_completed",
    ]


@pytest.mark.asyncio
async def test_llm_failure_keeps_job_pending_and_records_error(
    db_session_factory: TestSessionFactory,
    sqs_message_factory: Callable[..., dict[str, Any]],
    fake_llm_factory: Callable[..., Any],
):
    """LLM 호출이 실패하면 작업을 대기 상태로 유지하고 오류를 기록한다."""
    message = sqs_message_factory()
    llm = fake_llm_factory(error=TimeoutError("LLM timeout"))

    should_delete = await process_sqs_message(message, langchain_client=llm)

    job, events = await load_job_with_events(
        db_session_factory,
        "analysis-request-1",
    )
    # False는 worker가 SQS message를 삭제하지 않아 재전달될 수 있다는 뜻이다.
    assert should_delete is False
    assert len(llm.calls) == 1
    assert job.openai_state == OPENAI_STATE_PENDING
    assert job.postprocess_state == POSTPROCESS_STATE_NOT_STARTED
    assert job.submission_attempts == 1
    assert job.openai_error == "LLM timeout"
    assert job.openai_error_payload is not None
    assert [event.event_type for event in events] == [
        "sqs_received",
        "langchain_submit_failed",
    ]


@pytest.mark.asyncio
async def test_duplicate_completed_job_does_not_invoke_llm_again(
    db_session_factory: TestSessionFactory,
    sqs_message_factory: Callable[..., dict[str, Any]],
    fake_llm_factory: Callable[..., Any],
):
    """완료된 작업을 중복 수신하면 LLM을 다시 호출하지 않는다."""
    llm = fake_llm_factory()
    # 두 메시지는 MessageId만 다르고 기본 external_request_id는 같다.
    # 이는 SQS가 동일한 논리 작업을 다시 전달한 상황을 재현한다.
    first_message = sqs_message_factory(message_id="message-1")
    duplicate_message = sqs_message_factory(message_id="message-2")

    first_should_delete = await process_sqs_message(
        first_message,
        langchain_client=llm,
    )
    duplicate_should_delete = await process_sqs_message(
        duplicate_message,
        langchain_client=llm,
    )

    job, events = await load_job_with_events(
        db_session_factory,
        "analysis-request-1",
    )
    async with db_session_factory() as session:
        # count query로 중복 전달 뒤에도 Job 행이 하나뿐인지 직접 확인한다.
        job_count = await session.scalar(select(func.count()).select_from(Job))

    assert first_should_delete is True
    assert duplicate_should_delete is True
    assert job_count == 1
    assert len(llm.calls) == 1
    assert job.sqs_message_id == "message-2"
    assert [event.event_type for event in events] == [
        "sqs_received",
        "langchain_completed",
        "sqs_received",
    ]


@pytest.mark.asyncio
async def test_redelivered_failed_job_invokes_llm_again(
    db_session_factory: TestSessionFactory,
    sqs_message_factory: Callable[..., dict[str, Any]],
    fake_llm_factory: Callable[..., Any],
):
    """실패한 작업이 재전달되면 같은 Job으로 LLM을 다시 호출한다."""
    # 첫 호출은 일시적 실패, 같은 작업의 두 번째 전달은 성공하도록 서로 다른
    # fake를 사용한다. 완료된 중복 작업과 달리 실패 Job에는 response ID가 없어
    # 현재 구현은 LLM을 다시 호출한다.
    failed_llm = fake_llm_factory(error=TimeoutError("temporary failure"))
    recovered_llm = fake_llm_factory(content="재시도 성공")

    first_should_delete = await process_sqs_message(
        sqs_message_factory(message_id="message-1"),
        langchain_client=failed_llm,
    )
    retry_should_delete = await process_sqs_message(
        sqs_message_factory(message_id="message-2"),
        langchain_client=recovered_llm,
    )

    job, events = await load_job_with_events(
        db_session_factory,
        "analysis-request-1",
    )
    assert first_should_delete is False
    assert retry_should_delete is True
    assert len(failed_llm.calls) == 1
    assert len(recovered_llm.calls) == 1
    assert job.openai_state == OPENAI_STATE_COMPLETED
    assert job.submission_attempts == 2
    assert job.openai_error is None
    assert job.result_payload == {"analysis": "재시도 성공"}
    assert [event.event_type for event in events] == [
        "sqs_received",
        "langchain_submit_failed",
        "sqs_received",
        "langchain_completed",
    ]
