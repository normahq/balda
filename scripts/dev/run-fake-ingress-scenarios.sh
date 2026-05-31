#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"

PATTERN='TestCommandHandlerSubmitGoalTask_PublishesDurableCommandOnly|TestInboundWebhookReceiver_AcceptsAndPublishesCommand|TestInboundWebhookReceiver_SessionModePublishesSessionCommand|TestScheduledTaskSchedulerDispatchTask_PublishesCommandAndReschedules'

exec go test ./internal/apps/balda/handlers -run "${PATTERN}" "$@"
