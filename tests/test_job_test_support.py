from collections.abc import Callable

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Job


@pytest.mark.asyncio
async def test_database_fixture_persists_job(
    db_session_factory: async_sessionmaker[AsyncSession],
    job_factory: Callable[..., Job],
):
    # @pytest.mark.asyncio는 pytest가 async test를 event loop에서 실행하게 한다.
    # session.begin()이 정상 종료되면 transaction이 commit된다.
    async with db_session_factory() as session:
        async with session.begin():
            session.add(job_factory())

    # 새 session으로 다시 조회해 같은 session의 메모리가 아니라 실제 DB에
    # commit된 결과인지 확인한다.
    async with db_session_factory() as session:
        job_count = await session.scalar(select(func.count()).select_from(Job))

    assert job_count == 1
