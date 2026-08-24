import json
from collections.abc import AsyncIterator, Callable
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

_ = load_dotenv(".env.test", override=True)

# conftest.py는 pytest가 자동으로 읽으므로 아래 fixture를 테스트 파일에서
# import하지 않아도 테스트 함수의 매개변수 이름으로 요청할 수 있다.
# 환경변수를 먼저 읽어야 app.database가 테스트 설정으로 초기화되므로 app import는
# load_dotenv 호출 뒤에 둔다.
from app.database import Base  # noqa: E402
from app.models import Job  # noqa: E402
from app.services import job as job_service  # noqa: E402

TestSessionFactory = async_sessionmaker[AsyncSession]


@pytest_asyncio.fixture
async def db_session_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[TestSessionFactory]:
    """Service characterization tests용 격리 DB를 제공한다.

    SQLite로 service 상태 전이와 이벤트 저장을 확인하며 PostgreSQL 고유 query,
    constraint 또는 locking을 검증하는 fixture로 사용하지 않는다.
    """
    test_engine = create_async_engine(
        "sqlite+aiosqlite://",
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(test_engine, expire_on_commit=False)

    # production model과 같은 table을 테스트 전용 메모리 DB에 만든다.
    async with test_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    # service는 module 전역의 engine과 session factory를 직접 사용한다.
    # monkeypatch는 테스트 동안만 이를 테스트 DB로 바꾸고 종료 시 자동 복구한다.
    monkeypatch.setattr(job_service, "engine", test_engine)
    monkeypatch.setattr(job_service, "AsyncSessionLocal", session_factory)

    # fixture의 yield 앞은 setup, 뒤는 teardown이다. 테스트 함수에는
    # yield한 session_factory가 전달된다.
    yield session_factory

    await test_engine.dispose()


@pytest.fixture
def sqs_message_factory() -> Callable[..., dict[str, Any]]:
    # fixture가 메시지 자체가 아니라 생성 함수를 반환하므로 각 테스트가 필요한
    # 필드만 덮어쓰면서 서로 독립된 메시지를 만들 수 있다.
    def build_message(
        *,
        external_request_id: str = "analysis-request-1",
        message_id: str = "message-1",
        receipt_handle: str = "receipt-1",
        openai_request: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = {
            "external_request_id": external_request_id,
            "result_fetch_url": "https://pizza.example.com/analysis/result",
            "openai_request": openai_request
            or {
                "schema_version": "1.0",
                "context": {},
                "summary": {},
                "metrics": {},
            },
        }
        return {
            "MessageId": message_id,
            "ReceiptHandle": receipt_handle,
            "Body": json.dumps(body),
        }

    return build_message


class FakeLLMClient:
    """네트워크 호출 없이 LLM 성공, 실패와 호출 횟수를 재현한다."""

    def __init__(self, *, content: str = "분석 결과", error: Exception | None = None):
        self.content = content
        self.error = error
        self.calls: list[object] = []

    async def ainvoke(self, messages: object) -> SimpleNamespace:
        self.calls.append(messages)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(content=self.content)


@pytest.fixture
def fake_llm_factory() -> Callable[..., FakeLLMClient]:
    # **kwargs는 테스트가 content 또는 error 같은 이름 있는 인자를 전달하게 한다.
    return lambda **kwargs: FakeLLMClient(**kwargs)


class FakeSQSClient:
    """send_message 요청을 기록하고 설정된 결과 또는 오류를 반환한다."""

    def __init__(self, *, error: Exception | None = None):
        self.error = error
        self.requests: list[dict[str, Any]] = []

    def send_message(self, **request: Any) -> dict[str, str]:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return {"MessageId": "notify-message-1"}


@pytest.fixture
def fake_sqs_factory() -> Callable[..., FakeSQSClient]:
    return lambda **kwargs: FakeSQSClient(**kwargs)


class FakeOpenAIClient:
    """client.responses.retrieve(...) 형태의 OpenAI SDK 경계를 흉내 낸다."""

    def __init__(self, *, response: object = None, error: Exception | None = None):
        self.response = response
        self.error = error
        self.response_ids: list[str] = []
        self.responses = self

    async def retrieve(self, response_id: str) -> object:
        self.response_ids.append(response_id)
        if self.error is not None:
            raise self.error
        return self.response


@pytest.fixture
def fake_openai_factory() -> Callable[..., FakeOpenAIClient]:
    return lambda **kwargs: FakeOpenAIClient(**kwargs)


@pytest.fixture
def job_factory() -> Callable[..., Job]:
    # 기본값은 정상 Job 생성에 필요한 최소값이고, overrides로 시나리오별 상태를
    # 지정한다. 아직 DB에 저장하지 않으므로 테스트가 transaction 시점을 제어한다.
    def build_job(**overrides: Any) -> Job:
        values: dict[str, Any] = {
            "external_request_id": "analysis-request-1",
            "result_fetch_url": "https://pizza.example.com/analysis/result",
            "openai_request_payload": {
                "schema_version": "1.0",
                "context": {},
                "summary": {},
                "metrics": {},
            },
        }
        values.update(overrides)
        return Job(**values)

    return build_job
