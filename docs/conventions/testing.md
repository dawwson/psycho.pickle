# 테스트 작성 원칙

## 범위 선택

동작을 충분히 검증할 수 있는 가장 작은 범위를 사용합니다.

| 계층 | 대상 | 주요 검증 |
| --- | --- | --- |
| 단위 테스트 | 변환 함수, schema, 상태 계산 | 반환값, validation과 계산 규칙 |
| service characterization test | 실제 service 함수와 model | 현재 상태 전이, 외부 호출과 이벤트 기록 |
| PostgreSQL 통합 테스트 | 실제 SQLAlchemy session과 PostgreSQL | query, constraint, JSONB와 row locking |

PostgreSQL 고유 동작을 fake session이나 SQLite만으로 검증했다고 표현하지 않습니다.

## Fixture와 외부 의존성

- 테스트마다 DB 데이터와 fake 호출 기록을 격리합니다.
- service가 자체 session을 열고 commit하므로 transaction rollback에만 의존하지 말고 생성 데이터를 정리하거나 테스트별 DB를 사용합니다.
- 반복되는 `Job`, SQS message와 외부 응답은 목적이 드러나는 fixture로 만듭니다.
- LLM, SQS와 OpenAI recovery는 응답, 예외와 호출 횟수를 제어할 수 있는 작은 fake로 대체합니다.
- 재시도와 stale 판정은 고정 시각 또는 명확한 과거·미래 시각으로 검증합니다.

## 학습용 주석

- 테스트 함수명은 기존 스타일대로 영어를 사용하고, 함수 첫 줄의 한글 docstring으로 검증하는 비즈니스 시나리오를 설명합니다.
- Python과 pytest에 익숙하지 않은 개발자가 이해할 수 있도록 fixture 주입, `async`/`await`, context manager, transaction과 fake 사용 이유를 주석으로 설명합니다.
- 문법을 그대로 번역하지 않고 해당 기능이 테스트에서 맡는 역할과 생명주기를 설명합니다.
- 같은 문법이 반복되면 처음 등장하는 곳에서 자세히 설명하고 이후에는 시나리오의 차이만 설명합니다.

## Characterization test

- 현재 구현이 실제로 수행하는 동작만 고정하고 목표 동작을 expectation에 섞지 않습니다.
- `Job` 상태와 관련 `JobEvent`를 함께 검증합니다.
- 중복 처리에서는 `Job` 수와 LLM 호출 횟수처럼 멱등성을 직접 보여주는 값을 확인합니다.
- 결함을 fixture나 mock으로 숨기지 않습니다. 발견한 결함의 수정과 테스트 추가는 분리합니다.
- 테스트를 추가하면서 `app/services/job.py` 구조 변경이나 메시지 schema 변경을 수행하지 않습니다.
