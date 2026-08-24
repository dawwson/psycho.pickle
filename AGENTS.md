# Repository Guidelines

## Scope and Constraints

- Pizza는 분석 요청 lifecycle의 기준 상태를 소유하고 Pickle은 LLM 작업과 처리 이력을 소유합니다.
- 명시적인 요청 없이 Box 또는 Pizza API를 수정하지 않습니다.
- Pizza–Pickle 메시지 계약을 변경할 때는 양쪽 저장소의 DTO, 소비 코드, 문서와 테스트를 함께 확인합니다.
- SQS의 at-least-once 전달을 전제로 `external_request_id`와 결과 통지의 멱등성을 보존합니다.
- 현재 구현과 목표 구조를 구분합니다.
- 요청과 무관한 리팩터링을 피하고 기존 사용자 변경을 보존합니다.
- 사용자가 명시적으로 요청하기 전에는 staging, commit, push 또는 pull request를 수행하지 않습니다.

## Verification

Python 코드나 테스트를 변경하면 다음 명령을 실행합니다.

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

문서만 변경하면 링크와 예제 명령을 확인하고 `git diff --check`를 실행합니다. 동작 변경에는 같은 변경의 회귀 테스트를 포함합니다.

## Testing

- 테스트를 작성하거나 수정하기 전에 `docs/conventions/testing.md`를 확인합니다.
- 기본 테스트에서 실제 OpenAI API나 AWS를 호출하지 않습니다.

## Secrets and Configuration

- `.env.local.example`, `.env.test`, `app/config.py`와 배포 script의 환경변수 이름을 함께 확인합니다.
- `.env`, AWS credential, DB password, webhook secret과 OpenAI API key를 커밋하거나 로그에 출력하지 않습니다.
- 테스트에서 운영 DB, credential 또는 queue URL을 사용하지 않습니다.
