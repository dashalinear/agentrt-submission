# Отчёт: durable event-driven runtime для агента-составителя отчётов

Вариант стека: **custom runtime** — Python asyncio + SQLite (WAL) как durable state store + transactional outbox +
таблицы `events`, `timers`, `scheduler`; FastAPI + CLI. Архитектура, форматы, таблица переходов, правило гонок и
обоснование выбора — в [README.md](README.md). **Все числа ниже вычислены скриптом `experiments/make_report.py`
из сырых данных `results/raw/`** (симуляция), `results/real_llm/` (реальная LLM: Ollama, локально) и
`results/real_llm_server/` (реальная LLM: сервер ITMO); разделы Simulated и два прогона Real LLM не смешиваются.

## 1. Окружение

| параметр | значение |
|---|---|
| ОС | Windows-10-10.0.19045-SP0 |
| CPU | AMD64 Family 25 Model 116 Stepping 1, AuthenticAMD (16 логич. ядер) |
| RAM | 15.2 GB |
| Python | 3.13.7 |
| SQLite | 3.50.4 |
| fastapi | 0.162.0 |
| starlette | 1.10.0 |
| uvicorn | 0.54.0 |
| httpx | 0.28.1 |
| pydantic | 2.14.3 |
| pytest | 9.2.0 |
| matplotlib | 3.11.2 |
| numpy | 2.5.4 |
| Markdown | 3.11.3 |
| Масштаб времени, Part 1/2 | 1.0 (реальные секунды) |
| Масштаб времени, Part 3 | 0.1 → 1 virtual second = 0.1 real seconds |
| Seeds | Part 1: [1000, 1001, 1002, 1003, 1004]; Part 2: 7; Part 3: [11, 12, 13] (одинаковые для всех политик) |
| LLM в этих прогонах | **SimulatedLLM** (детерминированный); реальная LLM — раздел «Real LLM run» |

![states](results/figures/state_machine.png)

## 2. Автотесты

Итог (Windows): **57 passed, 1 warning in 27.90s** (полный вывод `pytest -v`: `results/pytest_output.txt`).

Тот же набор в Linux-контейнере (`Dockerfile`, POSIX SIGKILL в тестах восстановления): **57 passed, 1 warning in 27.99s** (`results/raw_linux/pytest_output.txt`).

```
tests/test_cancel.py::test_cancel_queued PASSED                          [  1%]
tests/test_cancel.py::test_cancel_waiting PASSED                         [  3%]
tests/test_cancel.py::test_cancel_running_stops_step_and_no_new_steps PASSED [  5%]
tests/test_cancel.py::test_late_step_result_after_cancel_is_discarded PASSED [  7%]
tests/test_events.py::test_duplicate_event_applied_once PASSED           [  8%]
tests/test_events.py::test_event_in_wrong_state_is_ignored_and_logged PASSED [ 10%]
tests/test_events.py::test_stale_approval_id_is_ignored PASSED           [ 12%]
tests/test_events.py::test_approval_after_cancel_or_timeout_does_not_change_state PASSED [ 14%]
tests/test_events.py::test_rejection_revises_then_fails PASSED           [ 15%]
tests/test_events.py::test_outbox_notifies_approval_request_and_terminal_state PASSED [ 17%]
tests/test_idempotency.py::test_create_job_same_key_returns_same_job PASSED [ 19%]
tests/test_idempotency.py::test_unique_index_is_the_backstop PASSED      [ 21%]
tests/test_idempotency.py::test_concurrent_creates_with_same_key_create_one_job PASSED [ 22%]
tests/test_idempotency.py::test_publisher_dedups_by_idempotency_key PASSED [ 24%]
tests/test_idempotency.py::test_publisher_ack_lost_then_retry_returns_previous_result PASSED [ 26%]
tests/test_idempotency.py::test_http_api_create_idempotent_and_429 PASSED [ 28%]
tests/test_idempotency.py::test_http_publisher_app PASSED                [ 29%]
tests/test_llm.py::test_simulated_llm_is_deterministic PASSED            [ 31%]
tests/test_llm.py::test_openai_compatible_success_and_usage PASSED       [ 33%]
tests/test_llm.py::test_openai_compatible_error_classification[429-TransientError] PASSED [ 35%]
tests/test_llm.py::test_openai_compatible_error_classification[503-TransientError] PASSED [ 36%]
tests/test_llm.py::test_openai_compatible_error_classification[400-PermanentError] PASSED [ 38%]
tests/test_llm.py::test_openai_compatible_error_classification[401-PermanentError] PASSED [ 40%]
tests/test_llm.py::test_openai_compatible_transport_error_is_transient PASSED [ 42%]
tests/test_races.py::test_deadline_beats_approval_when_timer_not_yet_fired PASSED [ 43%]
tests/test_races.py::test_cancel_beats_deadline_when_timer_not_yet_fired PASSED [ 45%]
tests/test_races.py::test_deadline_beats_step_completion PASSED          [ 47%]
tests/test_races.py::test_randomized_races_preserve_invariants PASSED    [ 49%]
tests/test_recovery.py::test_kill_between_steps_does_not_repeat_checkpointed_steps PASSED [ 50%]
tests/test_recovery.py::test_kill_during_llm_call_is_retried_after_lease_expiry PASSED [ 52%]
tests/test_recovery.py::test_publication_retracted_durably_when_job_cancelled_after_worker_crash PASSED [ 54%]
tests/test_recovery.py::test_side_effect_without_ack_is_not_duplicated PASSED [ 56%]
tests/test_retry.py::test_repeated_transient_error_bounded_then_failed PASSED [ 57%]
tests/test_retry.py::test_transient_then_success PASSED                  [ 59%]
tests/test_retry.py::test_permanent_error_fails_immediately PASSED       [ 61%]
tests/test_retry.py::test_token_budget_and_max_steps PASSED              [ 63%]
tests/test_retry.py::test_backoff_equal_jitter_bounds PASSED             [ 64%]
tests/test_retry.py::test_lease_loss_counts_as_attempt_poison_pill PASSED [ 66%]
tests/test_scheduling.py::test_global_and_per_tenant_limits_never_exceeded[fifo] PASSED [ 68%]
tests/test_scheduling.py::test_global_and_per_tenant_limits_never_exceeded[drr] PASSED [ 70%]
tests/test_scheduling.py::test_limits_hold_across_two_worker_processes_sharing_the_db PASSED [ 71%]
tests/test_scheduling.py::test_admission_control_rejects_with_reason PASSED [ 73%]
tests/test_scheduling.py::test_fifo_serves_heavy_backlog_first_drr_interleaves PASSED [ 75%]
tests/test_scheduling.py::test_aging_lets_old_low_priority_job_overtake PASSED [ 77%]
tests/test_smoke.py::test_happy_path_with_approval PASSED                [ 78%]
tests/test_states.py::test_allowed_transitions_table_matches_code PASSED [ 80%]
tests/test_states.py::test_terminal_row_cannot_be_updated_even_with_raw_sql[SUCCEEDED] PASSED [ 82%]
tests/test_states.py::test_terminal_row_cannot_be_updated_even_with_raw_sql[FAILED] PASSED [ 84%]
tests/test_states.py::test_terminal_row_cannot_be_updated_even_with_raw_sql[CANCELLED] PASSED [ 85%]
tests/test_states.py::test_terminal_row_cannot_be_updated_even_with_raw_sql[TIMED_OUT] PASSED [ 87%]
tests/test_states.py::test_invalid_transition_rejected_by_trigger PASSED [ 89%]
tests/test_states.py::test_state_change_requires_version_bump PASSED     [ 91%]
tests/test_states.py::test_cas_with_stale_version_fails PASSED           [ 92%]
tests/test_states.py::test_transition_log_written_by_trigger PASSED      [ 94%]
tests/test_timers_and_slots.py::test_deadline_in_queue_fires_after_restart PASSED [ 96%]
tests/test_timers_and_slots.py::test_deadline_while_waiting_survives_worker_restart PASSED [ 98%]
tests/test_timers_and_slots.py::test_waiting_job_does_not_occupy_execution_slot PASSED [100%]
```

**Структурированные логи.** Каждая запись — JSON с полями `job_id, step, attempt, event_id` (и `worker, tenant_id, state, data`). Пример (34 строк, один job с transient-ошибкой, дублем и поздним событием): `results/raw/sample_structured_log.jsonl`:

```
{"ts": 1794312559.234067, "level": "INFO", "logger": "agentrt.worker", "msg": "step start", "job_id": "job-0d0be54488dd4a2d", "step": "fetch_documents#0", "attempt": 1, "event_id": null, "worker": "w1:8f096185", "tenant_id": "tenant-demo", "state": null}
{"ts": 1794312559.247305, "level": "INFO", "logger": "agentrt.worker", "msg": "step start", "job_id": "job-0d0be54488dd4a2d", "step": "extract#0", "attempt": 1, "event_id": null, "worker": "w1:8f096185", "tenant_id": "tenant-demo", "state": null}
{"ts": 1794312559.262911, "level": "INFO", "logger": "agentrt.worker", "msg": "step start", "job_id": "job-0d0be54488dd4a2d", "step": "draft#0.0", "attempt": 1, "event_id": null, "worker": "w1:8f096185", "tenant_id": "tenant-demo", "state": null}
{"ts": 1794312559.293564, "level": "INFO", "logger": "agentrt.store", "msg": "transition", "job_id": "job-0d0be54488dd4a2d", "step": "draft#0.0", "attempt": 1, "event_id": null, "worker": null, "tenant_id": null, "state": "QUEUED", "data": {"src": "RUNNING", "dst": "QUEUED", "reason": "transient error at draft#0.0 (attempt 1): TransientError: simulated transient LLM error (503) on draft#0.0; retry in 0.017s"}}
{"ts": 1794312559.293739, "level": "WARNING", "logger": "agentrt.worker", "msg": "step transient error -> retry", "job_id": "job-0d0be54488dd4a2d", "step": "draft#0.0", "attempt": 1, "event_id": null, "worker": null, "tenant_id": null, "state": null, "data": {"error": "simulated transient LLM error (503) on draft#0.0"}}
{"ts": 1794312559.324831, "level": "INFO", "logger": "agentrt.worker", "msg": "step start", "job_id": "job-0d0be54488dd4a2d", "step": "draft#0.0", "attempt": 2, "event_id": null, "worker": "w1:8f096185", "tenant_id": "tenant-demo", "state": null}
```

## 3. Part 1 — восстановление и идемпотентность (Simulated)

**Время** (реальные секунды; «возобновление» = первый шаг job после рестарта):

| сценарий | N | итог | время job p50 [min–max] | + к baseline p50 | старт процесса → воркер готов p50 | воркер готов → возобновление p50 | старт процесса → возобновление p50 |
|---|---|---|---|---|---|---|---|
| без отказов (baseline) | 5 | SUCCEEDED: 5 | 2.83 [2.82–2.85] | – | – | – | – |
| 1. kill между шагами (после extract) | 5 | SUCCEEDED: 5 | 4.20 [4.19–4.23] | 1.36 | 1.16 | 0.000 | 1.16 |
| 2a. kill во время LLM, другой воркер ждёт lease | 5 | SUCCEEDED: 5 | 5.13 [5.12–5.16] | 2.30 | 1.17 | 0.832 | 2.00 |
| 2b. kill во время LLM, рестарт того же воркера | 5 | SUCCEEDED: 5 | 4.34 [4.30–4.43] | 1.51 | 1.18 | 0.001 | 1.19 |
| 3a. крэш после publish (ack не сохранён) | 5 | SUCCEEDED: 5 | 4.33 [4.25–4.38] | 1.49 | 1.31 | 0.001 | 1.32 |
| 3b. ack потерян в сети (503 после коммита) | 5 | SUCCEEDED: 5 | 3.33 [3.21–3.42] | 0.50 | – | – | – |
| 4. повторяющаяся transient-ошибка | 5 | FAILED: 5 | 7.06 [6.85–7.41] | 4.23 | – | – | – |

**Повторы и побочные эффекты** (суммы по N прогонам):

| сценарий | LLM-вызовов | повторных LLM | повторных tool | вызовов с неизвестным исходом | перевыполнено шагов с чекпоинтом | publish физ. / лог. | дублей лог. публикаций | доп. токены прерванных вызовов: подтверждено / верхняя граница | доп. стоимость USD: подтверждено / верх. |
|---|---|---|---|---|---|---|---|---|---|
| без отказов (baseline) | 10 | 0 | 0 | 0 | 0 | 5 / 5 | 0 | 0 / ≤0 | 0.000000 / ≤0.000000 |
| 1. kill между шагами (после extract) | 10 | 0 | 0 | 0 | 0 | 5 / 5 | 0 | 0 / ≤0 | 0.000000 / ≤0.000000 |
| 2a. kill во время LLM, другой воркер ждёт lease | 15 | 5 | 0 | 5 | 0 | 5 / 5 | 0 | 0 / ≤1498 | 0.000000 / ≤0.000675 |
| 2b. kill во время LLM, рестарт того же воркера | 15 | 5 | 0 | 5 | 0 | 5 / 5 | 0 | 0 / ≤1498 | 0.000000 / ≤0.000675 |
| 3a. крэш после publish (ack не сохранён) | 10 | 0 | 0 | 0 | 0 | 10 / 5 | 0 | 0 / ≤0 | 0.000000 / ≤0.000000 |
| 3b. ack потерян в сети (503 после коммита) | 10 | 0 | 0 | 0 | 0 | 10 / 5 | 0 | 0 / ≤0 | 0.000000 / ≤0.000000 |
| 4. повторяющаяся transient-ошибка | 20 | 15 | 0 | 0 | 0 | 0 / 0 | 0 | 0 / ≤0 | 0.000000 / ≤0.000000 |

Конфигурация: lease_timeout = 2.0 с, heartbeat = 0.25 с, max_attempts = 4, backoff base = 0.5 с, cap = 4.0 с; по 5 повторов на сценарий; воркер — отдельный процесс, убивается `Popen.kill()` (на Windows это `TerminateProcess` — аналог SIGKILL: без обработчиков и `finally`), FakePublisherService — отдельный HTTP-процесс. Сырые данные: `results/raw/recovery_runs.csv`, `recovery_step_runs.jsonl`, `recovery_calls.jsonl`.

![recovery](results/figures/recovery_job_time.png)

**Наблюдения (из таблицы):**

* **Kill между шагами.** Повторных LLM/tool вызовов: 0/0, перевыполненных шагов с чекпоинтом: 0. Шаги `fetch_documents`/`extract`, чей результат уже в `step_results`, не повторялись. Возобновление после того, как новый процесс готов: p50 = 0.000 с (тот же `--name` ⇒ lease прежней инкарнации забирается сразу при старте); основная часть времени «от старта процесса» (p50 1.16 с) — запуск интерпретатора и импорт модулей.
* **Kill во время LLM-вызова.** Каждый прерванный вызов повторён ровно один раз (повторных LLM: 5 на 5 прогонов в 2a и 5 на 5 в 2b). Если подхватывает *другой* воркер, он ждёт истечения lease: возобновление через 0.83 с после его готовности (остаток lease_timeout), против 0.001 с при рестарте того же воркера. **Неопределённость исхода:** 10 вызовов остались в журнале вызовов без завершения — рантайм не знает, обработал ли их провайдер и выставил ли счёт. Поэтому дополнительные токены даны интервалом: фактически оплаченные повторы, которые мы видим, = 0, верхняя граница (prompt + max_output_tokens прерванных вызовов) = 2996 токенов на 10 прогонов.
* **Side effect без ack.** В 3a процесс умирает сразу после ответа сервиса (до чекпоинта), в 3b сервис коммитит и отвечает 503. В обоих случаях физических запросов публикации 2 на job, логических — 1, дублей логических публикаций 0: повтор идёт с тем же `Idempotency-Key`, и сервис возвращает прежний `publication_id` (`replayed=true`).
* **Повторяющаяся transient-ошибка.** Ровно 4 попытки на прогон (всего 20 неудачных попыток в 5 прогонах), паузы между попытками (с, по прогонам): [0.46, 0.69, 1.31]; [0.47, 0.94, 1.07]; [0.46, 0.90, 1.17]; [0.41, 0.69, 1.15]; [0.43, 0.97, 1.42] — растут экспоненциально с jitter (ожидаемые интервалы [0.25–0.5], [0.5–1.0], [1.0–2.0] плюс время поллинга). Итог — `FAILED` с причиной: `step draft#0.0 failed after 4 attempts (max_attempts=4); last error: TransientError: simulated transient LLM error (503) on draft#0.0`.

### Part 1 на Linux: тот же эксперимент в Docker-контейнере (настоящий SIGKILL)

Окружение: Linux-6.18.58.1-microsoft-standard-WSL2-x86_64-with-glibc2.41, Python 3.13.18 (образ из `Dockerfile`, Docker Desktop на этой же машине, ядро WSL2). Код и параметры те же (`python -m experiments.exp_recovery --reps 5`), но `Popen.kill()` здесь посылает POSIX SIGKILL. Сырые данные — отдельно, в `results/raw_linux/`; с Windows-прогоном выше не смешиваются. Команда: `.\scripts\run_linux.ps1 -Parts recovery`.

| сценарий | N | итог | время job p50 [min–max], с | воркер готов → возобновление p50, с | повторных LLM | перевыполнено шагов с чекпоинтом | publish физ. / лог. | дублей лог. публикаций |
|---|---|---|---|---|---|---|---|---|
| без отказов (baseline) | 5 | SUCCEEDED: 5 | 2.76 [2.76–2.77] | – | 0 | 0 | 5 / 5 | 0 |
| 1. kill между шагами (после extract) | 5 | SUCCEEDED: 5 | 3.43 [3.43–3.46] | 0.000 | 0 | 0 | 5 / 5 | 0 |
| 2a. kill во время LLM, другой воркер ждёт lease | 5 | SUCCEEDED: 5 | 5.13 [5.12–5.13] | 1.607 | 5 | 0 | 5 / 5 | 0 |
| 2b. kill во время LLM, рестарт того же воркера | 5 | SUCCEEDED: 5 | 3.52 [3.51–3.54] | 0.000 | 5 | 0 | 5 / 5 | 0 |
| 3a. крэш после publish (ack не сохранён) | 5 | SUCCEEDED: 5 | 3.36 [3.35–3.40] | 0.000 | 0 | 0 | 10 / 5 | 0 |
| 3b. ack потерян в сети (503 после коммита) | 5 | SUCCEEDED: 5 | 3.20 [3.14–3.33] | – | 0 | 0 | 10 / 5 | 0 |
| 4. повторяющаяся transient-ошибка | 5 | FAILED: 5 | 7.25 [6.80–7.72] | – | 15 | 0 | 0 / 0 | 0 |

Гарантии воспроизвелись и под SIGKILL: шаги с чекпоинтом не перевыполняются, прерванный LLM-вызов повторяется ровно один раз, повторная публикация не создаёт дубля, transient-ошибка исчерпывает 4 попытки и даёт `FAILED`. Абсолютные времена отличаются от Windows (другой запуск процесса, файловая система тома). В 2a «готов → возобновление» — это остаток lease (2.0 с от последнего heartbeat), который новый воркер ещё ждёт после своей готовности: на Linux процесс готов быстрее (старт → готов p50 0.46 с против 1.17 с на Windows), поэтому ждать остаётся дольше (1.61 против 0.83 с), а время от старта процесса до возобновления близко (2.06 против 2.00 с).

## 4. Part 2 — событийный жизненный цикл (Simulated)

Все эксперименты — `time_scale = 1.0`, воркер в отдельном потоке (E1–E5, E7) или отдельном процессе (E6). Латентности измерены по меткам времени, записанным в БД в момент коммита (`events.received_at`, `transitions.ts`, `step_runs.started_at`). Сырые данные: `results/raw/events_*.csv`, `events_summary.json`.

### E1. Approval вовремя

100 job, итог: SUCCEEDED: 100; исходы событий: applied: 100.

| метрика | N | p50, мс | p95, мс | max, мс |
|---|---|---|---|---|
| event_to_transition_ms | 100 | 0.12 | 14.59 | 57.80 |
| client_roundtrip_ms | 100 | 0.66 | 15.14 | 58.41 |
| event_to_next_step_start_ms | 100 | 11.83 | 43.28 | 114.78 |

«event → state transition» — от приёма события до коммита перехода `WAITING_EVENT → QUEUED` (inbox и переход в одной транзакции). «event → next step started» дополнительно включает ожидание цикла планировщика (поллинг `poll_interval_s = 3 мс`, межпроцессного уведомления нет). Медиана — доли миллисекунды, а хвост (p95/max) — по всей видимости ожидание блокировки записи: если в этот момент транзакцию держит воркер, `BEGIN IMMEDIATE` отправителя попадает в busy-handler SQLite, который ждёт ступенями (1, 2, 5, 10, 15, 20, 25… мс). Это плата за single-writer хранилище.

![latency](results/figures/events_latency.png)

### E2. Approval не пришёл → TIMED_OUT

40 job с дедлайном 0.5 с: итог TIMED_OUT: 40. Запаздывание перехода относительно дедлайна: p50 = 4.147 мс, p95 = 8.301 мс, max = 8.886 мс (N = 40). Шагов, исполнявшихся во время ожидания: 0.

### E3. Дубли события

Отправлено 250 копий approval (на каждую job — 3 одновременно из разных потоков и 2 повторно позже, все с одним `event_id`): применено 50, распознано как дубли 200; job с более чем одним переходом `WAITING_EVENT → QUEUED`: 0.

### E4. Approval после CANCELLED / TIMED_OUT

40 поздних событий: исходы ignored_terminal: 40; повторный cancel: already_terminal: 40; изменилось состояние: 0, изменилась версия строки: 0.

### E5. Почти одновременные approval + cancel + deadline + завершение шагов

300 повторов. Для каждой job дедлайн T = создание + 0.6 с; approval отправляется в T + U(−150, +20) мс, cancel — в T + U(−60, +60) мс (seed 7); после approval воркер исполняет `save_report` и `publish` (~30–50 мс), так что их завершение тоже попадает в окно гонки. Плановые сдвиги и фактические моменты отправки (`approve_sent_ms`, `cancel_sent_ms` — по часам отправителя перед вызовом) записаны в `events_e5_races.csv`; на графике — фактические (они могут отличаться от плановых из-за планирования потоков ОС и ожидания блокировки записи; если job ещё не дошла до `WAITING_EVENT`, отправитель ждёт её, поэтому часть событий уходит позже дедлайна).

| итоговое состояние | N |
|---|---|
| CANCELLED | 128 |
| SUCCEEDED | 84 |
| TIMED_OUT | 88 |

Откуда пришли в терминал: QUEUED->CANCELLED: 20, QUEUED->TIMED_OUT: 11, RUNNING->CANCELLED: 72, RUNNING->SUCCEEDED: 84, RUNNING->TIMED_OUT: 50, WAITING_EVENT->CANCELLED: 36, WAITING_EVENT->TIMED_OUT: 27. Исходы approval: applied: 237, deadline_won: 15, ignored_terminal: 48. Исходы cancel: already_terminal: 172, cancelled: 128. Счётчики рантайма: cancel_after_terminal: 172, events_late: 48, late_results_discarded: 49.

Инварианты: job с ≠1 терминальным переходом — 0; публикаций, принадлежащих не-SUCCEEDED job и не отозванных — 0; SUCCEEDED без живой публикации — 0; публикаций, отозванных компенсацией — 46; попыток недопустимого перехода, отклонённых триггером — 0.

![races](results/figures/events_race_outcomes.png)

**Проверка правила гонок по каждой job** (из `events_e5_races.csv`, метки — моменты коммита в БД; N = 300):

| что было бы нарушением правила | job |
|---|---|
| SUCCEEDED закоммичен не раньше дедлайна (запрещено: шаг проверяет now ≥ deadline) | 0 |
| TIMED_OUT раньше дедлайна | 0 |
| CANCELLED без успешного cancel | 0 |
| cancel успешен, а итог не CANCELLED | 0 |
| approval `deadline_won`, а итог не TIMED_OUT | 0 |
| approval применён, хотя отправлен после дедлайна | 0 |
| больше одного терминального перехода | 0 |

Всего нарушений: 0. Отдельно: 37 job получили CANCELLED позже дедлайна (до 56.51 мс) — это допустимо по правилу: cancel не проверяет дедлайн и побеждает, если закоммичен раньше, чем таймер дедлайна (поллинг) успел перевести job в TIMED_OUT; первая закоммиченная терминальная транзакция выигрывает.

Картина соответствует правилу: cancel, закоммиченный раньше дедлайна и раньше завершения `publish`, даёт CANCELLED; если approval пришёл достаточно рано, шаги успевают закоммитить SUCCEEDED, и более поздние cancel получают `already_terminal`; если к моменту approval или коммита шага дедлайн уже прошёл, побеждает TIMED_OUT (`deadline_won`, `aborted_deadline`, `late_results_discarded`).

**Воспроизводимость E5.** Seed фиксирует только *плановые* моменты отправки; фактический порядок коммитов зависит от планирования потоков ОС и ожидания блокировки записи, поэтому распределение итоговых состояний (CANCELLED / SUCCEEDED / TIMED_OUT) от запуска к запуску заметно меняется (наблюдалось при контрольных перезапусках; их сырые данные в репозиторий не входят). Воспроизводимым результатом E5 являются инварианты (0 нарушений во всех прогонах) и соответствие каждого исхода правилу гонок, а не доли исходов.

**Исправление стенда E5 (найдено при прогоне на Linux).** В первой версии каждый из 20 потоков-отправителей сам опрашивал БД каждые 3 мс в том же процессе, что и воркер. На Windows это почти не влияло, а в Linux-контейнере из-за конкуренции за GIL воркер вытеснялся, job не успевали дойти до ожидания за 0.6 с, и все 300 повторов заканчивались TIMED_OUT — гонки не было (сырые данные того прогона перезаписаны исправленным; эффект воспроизводится диагностикой ниже). Диагностика `experiments/diag_e5_polling.py` (10 job, p50 пути «создание → `WAITING_EVENT`»): Linux — 5.04 с с 20 опрашивающими потоками против 0.09 с без них; Windows — 0.17 против 0.16 с (`results/raw_linux/diag_e5_polling.json`, `results/raw/diag_e5_polling.json`). Сейчас состояние опрашивает один поток-монитор, отправители ждут его сигнала; весь Part 2 перезапущен этим кодом на обеих ОС — данные выше и в блоке «на Linux» уже с исправленным стендом. Рантайм не менялся: это свойство стенда, где воркер и отправители живут в одном процессе.

### E6. Рестарт воркера во время ожидания

| вариант | повтор | итог | открытых step_runs в ожидании | состояние пока воркер мёртв | исход события | готов → первый шаг, мс | готов → терминал, мс | опоздание таймаута, мс |
|---|---|---|---|---|---|---|---|---|
| approval_while_down | 0 | SUCCEEDED | 0 | QUEUED | applied | 0.36 | 59.79 | – |
| deadline_while_down | 0 | TIMED_OUT | 0 | WAITING_EVENT | – | – | -0.78 | 1107.63 |
| approval_after_restart | 0 | SUCCEEDED | 0 | WAITING_EVENT | applied | 37.36 | 85.10 | – |
| approval_while_down | 1 | SUCCEEDED | 0 | QUEUED | applied | 0.39 | 58.19 | – |
| deadline_while_down | 1 | TIMED_OUT | 0 | WAITING_EVENT | – | – | -1.40 | 1102.90 |
| approval_after_restart | 1 | SUCCEEDED | 0 | WAITING_EVENT | applied | 22.64 | 75.47 | – |
| approval_while_down | 2 | SUCCEEDED | 0 | QUEUED | applied | 0.45 | 52.96 | – |
| deadline_while_down | 2 | TIMED_OUT | 0 | WAITING_EVENT | – | – | -0.79 | 1109.12 |
| approval_after_restart | 2 | SUCCEEDED | 0 | WAITING_EVENT | applied | 9.66 | 58.98 | – |
| approval_while_down | 3 | SUCCEEDED | 0 | QUEUED | applied | 0.37 | 64.41 | – |
| deadline_while_down | 3 | TIMED_OUT | 0 | WAITING_EVENT | – | – | -1.68 | 1129.73 |
| approval_after_restart | 3 | SUCCEEDED | 0 | WAITING_EVENT | applied | 15.18 | 63.06 | – |
| approval_while_down | 4 | SUCCEEDED | 0 | QUEUED | applied | 0.50 | 50.95 | – |
| deadline_while_down | 4 | TIMED_OUT | 0 | WAITING_EVENT | – | – | -0.95 | 1099.05 |
| approval_after_restart | 4 | SUCCEEDED | 0 | WAITING_EVENT | applied | 5.48 | 56.83 | – |

Ожидание не держит слот (0 открытых `step_runs`), событие принимается и применяется даже когда воркер мёртв (inbox в БД), а дедлайн, истёкший во время простоя, срабатывает при старте нового воркера (таймер хранится в БД). Для `deadline_while_down` «опоздание таймаута» ≈ длительность простоя — таймер не может сработать, пока нет ни одного живого воркера; отрицательное «готов → терминал» означает, что просроченный таймер сработал в процедуре старта воркера ещё до отметки готовности.

### E7. Время до реальной остановки после cancel

| отмена в состоянии | N | p50, мс | p95, мс | max, мс (QUEUED/WAITING: 0 по определению) |
|---|---|---|---|---|
| QUEUED | 15 | 0.00 | 0.00 | 0.00 |
| RUNNING | 15 | 63.48 | 93.38 | 100.83 |
| WAITING_EVENT | 15 | 0.00 | 0.00 | 0.00 |

Исходы LLM-вызовов в E7: cancelled: 15, ok: 15. В `RUNNING` остановка ограничена интервалом heartbeat (0.1 с в E7): шаг узнаёт об отмене на следующем heartbeat и прерывает LLM-вызов; в `QUEUED` и `WAITING_EVENT` исполняемой работы нет — остановка мгновенная (коммит перехода).

### Сводка инвариантов (переходы из журнала `transitions`, пишется триггером)

| эксперимент | проверено переходов | недопустимых переходов | изменений терминального статуса | разрывов версии | job с >1 терминальным переходом | журнал ≠ состояние |
|---|---|---|---|---|---|---|
| E1 | 1400 | 0 | 0 | 0 | 0 | 0 |
| E2 | 400 | 0 | 0 | 0 | 0 | 0 |
| E3 | 700 | 0 | 0 | 0 | 0 | 0 |
| E4 | 400 | 0 | 0 | 0 | 0 | 0 |
| E5 | 3749 | 0 | 0 | 0 | 0 | 0 |
| E7 | 285 | 0 | 0 | 0 | 0 | 0 |

### Part 2 на Linux (Docker-контейнер)

Окружение: Linux-6.18.58.1-microsoft-standard-WSL2-x86_64-with-glibc2.41, Python 3.13.18; те же скрипт и параметры (`exp_events --race-reps 300`, seed 7). Сырые данные отдельно — `results/raw_linux/events_*`; с Windows-прогоном выше не смешиваются (команда: `.\scripts\run_linux.ps1`).

| эксперимент | результат на Linux |
|---|---|
| E1 approval вовремя | SUCCEEDED: 100; event → transition p50 0.119 / p95 3.75 мс (N = 100) |
| E2 approval не пришёл | TIMED_OUT: 40; опоздание таймаута p50 3.025 / p95 4.716 мс (N = 40); шагов во время ожидания: 0 |
| E3 дубли события | отправлено 250, применено 50, дублей 200; job с >1 переходом: 0 |
| E4 approval после терминала | 40 поздних: ignored_terminal: 40; изменений состояния: 0 |
| E5 гонки | CANCELLED: 61, SUCCEEDED: 185, TIMED_OUT: 54; job с ≠1 терминальным переходом: 0; неотозванных публикаций не-SUCCEEDED job: 0; нарушений правила гонок по каждой job: 0; CANCELLED позже дедлайна (допустимо): 3 |
| E6 рестарт во время ожидания | 15 из 15 прогонов с ожидаемым итогом и 0 открытых step_runs |
| E7 остановка после cancel в RUNNING | p50 53.29 / p95 73.566 мс (N = 15) |
| инварианты E1–E5, E7 | проверено переходов 7088, нарушений 0 |

Поведение совпадает с Windows по всем проверкам (итоги E1–E4, E6, нулевые нарушения инвариантов). Латентности и доли исходов E5 различаются — они зависят от планировщика ОС и файловой системы тома (см. «Воспроизводимость E5»).

## 5. Part 3 — backpressure и честное планирование (Simulated)

**Нагрузка.** 3 пользователя (`heavy` шлёт сильно больше остальных), 2 класса: short ≈ 1.00 c и long ≈ 9.50 c номинальной работы (long = 30% заявок; long разбит на шаги ≤ 1.5 с), 10% заявок с priority = 1. Фазы (заявок в вирт. секунду): P1_below [0–150 с): heavy 0.5/с, light1 0.2/с, light2 0.2/с; P2_burst [150–240 с): heavy 3.0/с, light1 0.2/с, light2 0.2/с; P3_recovery [240–480 с): heavy 0.3/с, light1 0.2/с, light2 0.2/с. Пуассоновские прибытия, трасса генерируется из seed и сохраняется (`results/raw/load_arrivals_seed*.csv`) — **один и тот же поток заявок для всех политик**. Ресурсы одинаковые: `global_concurrency = 6` (номинальная ёмкость ≈ 1.69 job/с), `max_queue_size = 150` (admitted QUEUED+RUNNING), дедлайны short 60 с / long 200 с. **Масштаб времени: 1 вирт. с = 0.1 реальной с** (все времена ниже — виртуальные секунды). Seeds [11, 12, 13], по одному прогону каждой политики на seed.

Политики:
* `fifo` — общая очередь по `ready_at`, без лимита на tenant (`per_tenant_concurrency = global`);
* `drr` — deficit round robin по tenant (веса 1:1:1, quantum 1 вирт. с работы) + aging приоритетов (+1 за 10 с ожидания), без жёсткого лимита на tenant (work-conserving);
* `drr_iso` — `drr` + `per_tenant_concurrency = 3` + квота очереди `max_queue_per_tenant = 100` (жёсткая изоляция).

### Ожидание по user × class (ожидание = суммарное время в очереди в состоянии «готов к запуску», по всем шагам; для истёкших в очереди — до момента истечения)

| политика | user | class | N | p50 ожидания, с | p95 ожидания, с | p95 по seed (min–max) | max, с | истекло (TIMED_OUT) | e2e p50 успешных, с |
|---|---|---|---|---|---|---|---|---|---|
| fifo | heavy | short | 713 | 43.14 | 59.13 | 59.09–59.16 | 59.28 | 307 | 1.46 |
| fifo | heavy | long | 341 | 71.54 | 115.45 | 94.66–117.14 | 118.85 | 0 | 81.36 |
| fifo | light1 | short | 191 | 0.11 | 59.08 | 59.04–59.10 | 59.14 | 30 | 1.21 |
| fifo | light1 | long | 74 | 0.65 | 110.56 | 88.63–114.30 | 117.98 | 0 | 10.36 |
| fifo | light2 | short | 208 | 0.11 | 59.08 | 59.03–59.10 | 59.16 | 26 | 1.23 |
| fifo | light2 | long | 72 | 0.98 | 112.63 | 73.52–116.55 | 117.22 | 0 | 10.85 |
| drr | heavy | short | 754 | 58.97 | 59.67 | 59.17–59.67 | 59.70 | 395 | 1.29 |
| drr | heavy | long | 358 | 86.87 | 155.21 | 125.09–157.87 | 159.78 | 0 | 96.41 |
| drr | light1 | short | 189 | 0.12 | 1.21 | 1.04–1.44 | 2.85 | 0 | 1.29 |
| drr | light1 | long | 74 | 0.93 | 2.47 | 2.20–3.73 | 3.95 | 0 | 10.69 |
| drr | light2 | short | 208 | 0.11 | 1.62 | 1.03–3.27 | 4.08 | 0 | 1.31 |
| drr | light2 | long | 76 | 0.94 | 4.14 | 1.73–5.48 | 5.72 | 0 | 10.59 |
| drr_iso | heavy | short | 622 | 59.07 | 59.68 | 59.68–59.68 | 59.77 | 358 | 1.44 |
| drr_iso | heavy | long | 306 | 107.91 | 188.02 | 162.93–190.69 | 191.11 | 11 | 111.60 |
| drr_iso | light1 | short | 200 | 0.03 | 1.15 | 0.30–2.05 | 4.12 | 0 | 1.20 |
| drr_iso | light1 | long | 76 | 0.14 | 1.82 | 0.66–2.52 | 3.89 | 0 | 10.00 |
| drr_iso | light2 | short | 218 | 0.03 | 0.88 | 0.38–2.35 | 5.72 | 0 | 1.20 |
| drr_iso | light2 | long | 78 | 0.07 | 2.97 | 0.81–4.70 | 4.99 | 0 | 9.90 |

![p95](results/figures/load_wait_p95.png)

### Ожидание по приоритету (aging и защита от голодания; все 3 seed)

| политика | пользователи | priority | N | p50 ожидания, с | p95 ожидания, с | max, с | SUCCEEDED | TIMED_OUT |
|---|---|---|---|---|---|---|---|---|
| fifo | heavy | 0 | 963 | 47.25 | 107.90 | 118.85 | 683 | 280 |
| fifo | heavy | 1 | 91 | 49.00 | 111.35 | 118.22 | 64 | 27 |
| fifo | light1+light2 | 0 | 509 | 0.35 | 73.02 | 117.98 | 456 | 53 |
| fifo | light1+light2 | 1 | 36 | 0.04 | 70.51 | 117.22 | 33 | 3 |
| drr | heavy | 0 | 1015 | 59.06 | 143.43 | 159.78 | 625 | 390 |
| drr | heavy | 1 | 97 | 0.68 | 70.67 | 91.46 | 92 | 5 |
| drr | light1+light2 | 0 | 512 | 0.24 | 2.28 | 5.72 | 512 | 0 |
| drr | light1+light2 | 1 | 35 | 0.12 | 1.72 | 2.25 | 35 | 0 |
| drr_iso | heavy | 0 | 849 | 59.12 | 181.25 | 191.11 | 495 | 354 |
| drr_iso | heavy | 1 | 79 | 0.81 | 107.21 | 119.68 | 64 | 15 |
| drr_iso | light1+light2 | 0 | 533 | 0.03 | 1.74 | 5.72 | 533 | 0 |
| drr_iso | light1+light2 | 1 | 39 | 0.03 | 0.24 | 0.38 | 39 | 0 |

В `fifo` приоритет не учитывается: у `heavy` p50 ожидания priority 0 и 1 почти одинаковы (47.25 и 49.00 с). В `drr` priority = 1 внутри tenant обслуживается первым (p50 0.68 с против 59.06 с у priority 0), но задачи priority 0 не вытесняются бесконечно: aging (+1 к эффективному приоритету за 10 с ожидания) возвращает их в оборот — 625 из 1015 завершились успешно. Остальные (390) истекли по дедлайну: при длительной перегрузке «защита от голодания» не создаёт ёмкость, и дедлайн снимает в первую очередь давно ждущую низкоприоритетную работу перегружающего tenant. У лёгких пользователей приоритет почти не влияет — их спрос ниже доли. Для priority = 1 наблюдений мало (10 % заявок), см. столбец N.

### Сводка по политикам (сумма/максимум по 3 прогонам)

| политика | прогонов | заявок | принято | отклонено (по user) | SUCCEEDED | TIMED_OUT (по user) | jobs/s (весь прогон) | jobs/s в burst | загрузка слотов в burst | доля слот-времени на job, которые потом истекли | max суммарное ожидание job, с | max возраст старейшей job в очереди (от создания), с | max ожидание старейшего готового шага (от готовности), с | max длина очереди | max одновременно global / лимит | max на tenant / лимит |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo | 3 | 1811 | 1599 | 212 (heavy: 185, light1: 11, light2: 16) | 1236 | 363 (heavy: 307, light1: 30, light2: 26) | 0.853 | 0.667 | 98.4% | 5.9% | 118.85 | 128.16 | 16.54 | 145 | 6 / 6 | 6 / 6 |
| drr | 3 | 1811 | 1659 | 152 (heavy: 127, light1: 13, light2: 12) | 1264 | 395 (heavy: 395) | 0.872 | 0.730 | 98.4% | 5.2% | 159.78 | 168.84 | 31.63 | 144 | 6 / 6 | 6 / 6 |
| drr_iso | 3 | 1811 | 1500 | 311 (heavy: 311) | 1131 | 369 (heavy: 369) | 0.780 | 0.570 | 74.2% | 5.9% | 191.11 | 200.00 | 30.58 | 99 | 6 / 6 | 3 / 3 |

Масштабирование времени добавляет накладные расходы: медиана (фактическая − номинальная длительность шага) = 0.023 вирт. с, p95 = 0.123 вирт. с (транзакции SQLite и цикл asyncio в реальном времени не масштабируются).

### Доля выполненной работы во время перегрузки (окно burst 150–240 с, все seed)

|  | heavy | light1 | light2 |
|---|---|---|---|
| спрос (номинальная работа заявок) | 88.5% | 5.2% | 6.4% |
| вес / квота | 33.3% | 33.3% | 33.3% |
| fifo | 87.7% | 5.8% | 6.5% |
| drr | 78.6% | 9.3% | 12.1% |
| drr_iso | 66.4% | 14.7% | 18.9% |

![share](results/figures/load_work_share.png)

### Длина очереди и активные шаги во времени

![cmp](results/figures/load_queue_by_policy.png)

![ts-fifo](results/figures/load_timeseries_fifo.png)
![ts-drr](results/figures/load_timeseries_drr.png)
![ts-drr_iso](results/figures/load_timeseries_drr_iso.png)

### Интерпретация

* **Изоляция.** В FIFO burst пользователя `heavy` заполняет общую очередь, и короткие задачи лёгких пользователей стоят за ней — их p95 ожидания вырастает до десятков секунд и часть их задач истекает или отклоняется. DRR выбирает следующий шаг по кругу между tenant, поэтому лёгкие пользователи, чей спрос ниже их доли (33 %), получают всё, что просят, а перегрузка ложится на виновника:
  * light1: short p95 59.08 → 1.21 с, long p95 110.56 → 2.47 с (FIFO → DRR);
  * light2: short p95 59.08 → 1.62 с, long p95 112.63 → 4.14 с (FIFO → DRR);
  * heavy: short p95 59.13 → 59.67 с, long p95 115.45 → 155.21 с.
* **Throughput vs latency vs isolation.** `fifo` и `drr` work-conserving: загрузка слотов в burst 98.4% и 98.4%, DRR перераспределяет ожидание (и истечения) в сторону виновника перегрузки; число успешных job у DRR даже немного выше, т.к. короткие задачи лёгких пользователей не истекают. Throughput в job/с при одинаковой загрузке слотов не одинаков: в окне burst у `fifo` завершилось 180 job (из них long — 39), у `drr` — 197 (long — 56). Часть слот-времени уходит на шаги job, которые затем истекают по дедлайну (столбец «доля слот-времени…»: 5.9% у `fifo`, 5.2% у `drr`, 5.9% у `drr_iso`) — это работа, потерянная из-за перегрузки; остальная разница связана с тем, какие job успевают завершиться внутри окна, и отдельно не раскладывалась. `drr_iso` не даёт `heavy` занять больше 3 из 6 слотов даже когда остальные простаивают: загрузка слотов в burst 74.2%, throughput в burst 0.570 против 0.730 job/с у `drr` — плата за гарантированный запас ёмкости и за то, что лёгкие пользователи вообще не получают отказов (все 311 отказов `drr_iso` — у `heavy`). Квота очереди на tenant дополнительно защищает лёгких пользователей от отказов `queue_full`, которые в `fifo`/`drr` вызывает чужой burst (общий `max_queue_size`).
* **Чтение таблиц.** p95 ожидания `heavy` short ≈ 59–60 с во всех политиках — это цензурирование дедлайном 60 с (задачи истекают в очереди, см. TIMED_OUT); «e2e p50 успешных» считается только по выжившим задачам (survivorship), поэтому у `heavy` он мал при большом ожидании. Возраст старейшей job считается от её создания до последнего выхода из очереди (старт последнего шага или истечение в очереди); он ограничен дедлайном (60 с short / 200 с long) и у DRR больше, чем у FIFO: FIFO старит всех одинаково, DRR намеренно держит в очереди именно backlog `heavy`. Отдельный столбец «ожидание старейшего готового шага» (и график ниже) — возраст в очереди от момента готовности очередного шага; он сбрасывается после каждого шага, поэтому меньше.
* **Длительная перегрузка.** Admission control ограничивает очередь (429/REJECTED с причиной), дедлайны снимают job, которые не успеют, поэтому очередь и возраст старейшей job ограничены сверху, а не растут бесконечно; после окончания burst очередь рассасывается (фаза P3). Цена: почти все истёкшие job истекли в состоянии QUEUED *между* шагами, уже выполнив часть работы (1127 из 1127 истёкших по всем политикам успели исполнить хотя бы один шаг), — это и есть потерянное слот-время из сводной таблицы. Ранний отказ при admission дешевле, чем позднее истечение.
* **Планирование на границах шагов и ожидание коротких job.** После каждого шага job возвращается в очередь (`RUNNING → QUEUED`, `ready_at = now`) и заново конкурирует за слот. Поэтому шаг, стоящий первым в очереди, ждёт не всю 10-секундную long-job, а лишь освобождения любого слота — не дольше самого длинного шага (≤ 1.5 вирт. с в этой нагрузке; с реальной LLM шаг ограничен только `step_timeout_s` = 30 с). Но заходов столько, сколько шагов (медиана у успешных short — 6, у long — 9), и при backlog каждое повторное ожидание включает очередь перед job: под FIFO суммарное ожидание короткой job растёт как «число шагов × длина очереди» (p50 ожидания `heavy` short — десятки секунд, см. таблицу). DRR ограничивает это для лёгких пользователей, но не для перегружающего tenant. Цена шага как единицы планирования — лишняя транзакция claim/complete на каждый шаг (см. накладные расходы выше).
* **Aging.** Внутри tenant priority = 1 обгоняет priority = 0, но задача с низким приоритетом, прождавшая 10 с, получает +1 и не голодает (`tests/test_scheduling.py::test_aging_lets_old_low_priority_job_overtake`; измерения под нагрузкой — таблица «Ожидание по приоритету» выше).
* **Лимиты.** По трассе `step_runs` (интервалы исполнения) максимальная одновременность ни разу не превысила global/per-tenant лимит (столбцы «max … / лимит»; то же проверяют автотесты).
* **Ожидание approval не занимает слоты** — подтверждено автотестом `tests/test_timers_and_slots.py::test_waiting_job_does_not_occupy_execution_slot` (при `global_concurrency=1` вторая job полностью выполняется, пока первая ждёт approval) и E2/E6 (0 открытых `step_runs` в ожидании).

### Part 3 на Linux (Docker-контейнер, те же seed и поток заявок)

Окружение: Linux-6.18.58.1-microsoft-standard-WSL2-x86_64-with-glibc2.41, Python 3.13.18; seeds [11, 12, 13], по одному прогону каждой политики на seed, масштаб времени 0.1. Сырые данные отдельно — `results/raw_linux/load_*`; с Windows-прогоном не смешиваются.

| политика | прогонов | заявок | отклонено (по user) | SUCCEEDED | TIMED_OUT (по user) | jobs/s | загрузка слотов в burst | p95 ожидания light1 short, с | p95 ожидания heavy short, с | доля работы в burst heavy / light1 / light2 | max global / лимит | max на tenant / лимит |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo | 3 | 1811 | 209 (heavy: 181, light1: 12, light2: 16) | 1258 | 344 (heavy: 299, light1: 24, light2: 21) | 0.868 | 98.9% | 59.09 (N = 189) | 59.13 (N = 722) | 87.4% / 5.7% / 6.9% | 6 / 6 | 6 / 6 |
| drr | 3 | 1811 | 140 (heavy: 117, light1: 11, light2: 12) | 1283 | 388 (heavy: 388) | 0.885 | 99.0% | 1.11 (N = 191) | 59.67 (N = 764) | 79.9% / 9.1% / 11.0% | 6 / 6 | 6 / 6 |
| drr_iso | 3 | 1811 | 309 (heavy: 309) | 1131 | 371 (heavy: 371) | 0.780 | 74.1% | 1.02 (N = 200) | 59.69 (N = 621) | 66.9% / 14.6% / 18.6% | 6 / 6 | 3 / 3 |

Выводы Part 3 на Linux те же: DRR снижает p95 ожидания лёгких пользователей до единиц секунд, перегрузка (отказы и истечения) концентрируется у `heavy`, лимиты параллельности не превышаются, `drr_iso` жертвует загрузкой слотов ради изоляции.

## 6. Real LLM vs Simulated

### Simulated (данные этого отчёта)

| метрика | SimulatedLLM |
|---|---|
| LLM-вызовов на job (baseline) | 2.0 |
| токенов на job (prompt+completion) | 345.8 |
| стоимость на job, USD (иллюстративная цена из конфига) | 0.000118 |
| латентность LLM-вызова | задана конфигом: 1.0 с (Part 1) |
| время job baseline p50, с | 2.83 |

### Real LLM run

> **Статус: ЗАПУЩЕН** (данные из `results/real_llm/`, не смешиваются с симуляцией).

| метрика | Real LLM (Ollama, локально) |
|---|---|
| модель / endpoint | qwen3.5:4b / http://localhost:11434/v1 |
| job: итог | SUCCEEDED: 3 |
| время job end-to-end, с (min–max) | 77.34–88.50 (N = 3) |
| LLM-вызовов (ok / всего) | 6 / 6 |
| латентность LLM-вызова p50 / max, с | 27.88 / 33.39 (N = 6) |
| токенов prompt / completion | 702 / 919 |
| стоимость, USD | 0.000657 — условная (цена из конфига; локальная модель, реальных затрат нет) |
| recovery baseline (без отказов): время job, с / повторных LLM | 23.467 / 0 |
| kill во время LLM: resume после spawn, с | 1.184 |
| kill во время LLM: повторных вызовов / доп. токены прерванного вызова (подтверждено / верхняя граница) | 1 / 0 / ≤299 |
| крэш после publish: физ./лог. публикаций / resume после spawn, с | 2 / 1 / 1.655 |

В основном прогоне 3 job стартуют одновременно, а локальная Ollama на CPU обрабатывает запросы по очереди (в `results/real_llm/calls.jsonl` каждый следующий вызов завершается вплотную к предыдущему), поэтому латентность вызова и время job включают ожидание в очереди модели; в сценариях восстановления job одна, отсюда меньшее время baseline.

Чем реальная LLM отличается от симулятора (ожидаемо, проверить по таблице): латентность не детерминирована; ошибки 429/5xx/таймауты классифицируются как transient и ретраятся; прерванный крэшем вызов почти наверняка оплачен провайдером (верхняя граница доп. токенов становится реальной ценой); текст черновика при повторе может отличаться, но это не ломает идемпотентность публикации — тело собирается из *сохранённых* результатов шагов, а повтор `draft` после чекпоинта невозможен.

Запросы к реальной LLM идут с `temperature = 0` (`src/agentrt/llm.py`), поэтому в сценариях восстановления одного прогона (одинаковые документы и промпты) число completion-токенов совпадает между сценариями (см. `recovery_calls_real.jsonl`); между тремя job основного прогона промпты различаются, и токены разные (`calls.jsonl`). Для прерванного вызова в журнале записан только prompt (ответа не было).

### Real LLM run: сервер ITMO (qwen3.8-27b)

> **Статус: ЗАПУЩЕН** (данные из `results/real_llm_server/`; не смешиваются ни с симуляцией, ни с прогоном на qwen3.5:4b).

| метрика | Real LLM (сервер ITMO) |
|---|---|
| модель / endpoint | qwen3.8-27b / <внутренний адрес сервера университета, доступ через VPN>/v1 |
| job: итог | SUCCEEDED: 3 |
| время job end-to-end, с (min–max) | 6.24–6.36 (N = 3) |
| LLM-вызовов (ok / всего) | 6 / 6 |
| латентность LLM-вызова p50 / max, с | 1.72 / 1.98 (N = 6) |
| токенов prompt / completion | 702 / 910 |
| стоимость, USD | не считалась (тариф сервера неизвестен; цены в прогоне = 0) |
| recovery baseline (без отказов): время job, с / повторных LLM | 3.061 / 0 |
| kill во время LLM: resume после spawn, с | 1.315 |
| kill во время LLM: повторных вызовов / доп. токены прерванного вызова (подтверждено / верхняя граница) | 1 / 0 / ≤299 |
| крэш после publish: физ./лог. публикаций / resume после spawn, с | 2 / 1 / 1.37 |

Один повтор на сценарий восстановления — это иллюстрация, а не статистика. Латентность и время job нельзя напрямую сравнивать с прогоном на Ollama: другая модель и другое железо (CPU против сервера университета). Столбцы `cost_*` в `results/real_llm_server/recovery_runs_real.csv` посчитаны подпрограммой восстановления по иллюстративным ценам из конфига по умолчанию и реальной стоимостью не являются.

## 7. Ответы на вопросы задания

**1. Где хранится состояние?** В одном файле SQLite (WAL) рантайма: `jobs` (состояние, версия, lease,
следующий шаг, счётчики), `step_results` (чекпоинты шагов), `events`/`event_receipts` (inbox), `outbox`,
`timers` (дедлайны), `scheduler` (DRR), `transitions` (журнал, пишется триггером), `step_runs` (трасса
исполнения и занятые слоты). В памяти воркера нет ничего, что нельзя потерять: после SIGKILL всё
восстанавливается из БД. Внешний сервис публикаций хранит свои публикации в своей БД.

**2. Что при сбое между обновлением состояния и публикацией следующего события?** Такого окна нет:
обновление состояния и запись сообщения в `outbox` (`approval.requested`, `job.<terminal>`) — одна
транзакция; следующий шаг тоже не «событие в брокере», а поле `next_step` в той же строке. Если процесс
упал после коммита, но до доставки, relay при рестарте найдёт строку `delivered_at IS NULL` и доставит её
(at-least-once); получатель дедуплицирует по `message_id` (`Idempotency-Key`). Если упал до коммита —
не было ни нового состояния, ни сообщения; шаг повторится после истечения lease. Окно, которое транзакцией
закрыть нельзя, — внешний вызов: сервис публикаций уже закоммитил публикацию, а наш чекпоинт шага `publish`
ещё нет. Его закрывает не транзакция, а идемпотентность: повтор шага идёт с тем же `Idempotency-Key` и получает
прежнюю публикацию (Part 1, сценарии 3a/3b); если же job тем временем отменена/истекла, отзыв публикации
записывается в `outbox` вместе с терминальным переходом (вопрос 4).

**3. Какие операции могут повторяться и как ограничены последствия?** Повторяется только незакоммиченная
попытка шага: LLM-вызов, tool-вызов, publish, а также доставка outbox. Ограничения: (а) чекпоинт — шаг с
сохранённым результатом не перевыполняется; (б) `max_attempts` на шаг (крэш тоже считается попыткой —
защита от poison pill), backoff с jitter, `step_timeout`, `max_steps`, `max_tokens`; (в) внешний side effect
идемпотентен (`Idempotency-Key`), поэтому повтор publish даёт 0 дублей логических публикаций; (г) повтор
LLM стоит денег — измерено как доп. вызовы/токены (Part 1), с верхней границей для вызовов с неизвестным
исходом; (д) фенсинг по `lease_token` не даёт «зомби»-воркеру закоммитить поздний результат.

**4. Как разрешаются гонки cancel / deadline / completion?** Побеждает первая закоммиченная транзакция
перехода в терминальное состояние (SQLite сериализует писателей, переход — CAS по `version` с
`state NOT IN terminal`, триггер запрещает менять терминальные строки). При «одновременности» действует
приоритет CANCELLED > TIMED_OUT > approval > step completion: cancel не проверяет дедлайн; approval и коммит
шага проверяют `now ≥ deadline` и вместо себя коммитят TIMED_OUT; коммит шага фенсится lease. Поздние
результаты отбрасываются, поздние события получают `ignored_terminal`. Если publish мог пройти, а job
отменена/истекла/упала, выполняется компенсация — идемпотентный retract публикации; он записывается в `outbox`
в той же транзакции, что и терминальный переход, поэтому переживает падение воркера (автотест
`test_publication_retracted_durably_when_job_cancelled_after_worker_crash`). E5: 0 нарушений инвариантов и 0
нарушений правила по каждой job на всех повторах (таблица в E5).

**5. Что обеспечивает справедливость и что при длительной перегрузке?** DRR по tenant на границах шагов
(веса, quantum в секундах работы) + aging внутри tenant; опционально жёсткие per-tenant лимиты слотов и
квоты очереди. При длительной перегрузке очередь ограничена admission control (429/REJECTED с причиной и
метрикой), дедлайны снимают задачи, которые не успеют (TIMED_OUT, в т.ч. из очереди), DRR не даёт одному
tenant вытеснить остальных. `fifo` и `drr` work-conserving — в burst слоты заняты почти полностью (столбец
«загрузка слотов в burst» в Part 3), а DRR только перераспределяет ожидание, отказы и истечения в сторону
tenant-источника перегрузки; `drr_iso` сознательно не work-conserving: жёсткий per-tenant лимит оставляет часть
ёмкости свободной ценой throughput, зато лёгкие пользователи не получают ни отказов, ни истечений (Part 3).
Throughput в job/с не равен номинальной ёмкости из Part 3 (она посчитана для смеси заявок): он зависит от того,
какая доля выполненных задач — long; отдельно эта разница не анализировалась.

**6. Какие гарантии от инфраструктуры, а какие из кода?** Инфраструктура (SQLite): атомарность,
сериализуемость писателей, долговечность коммита (WAL), уникальные индексы и триггеры. Код: модель
состояний и CAS, lease/heartbeat/reaper/recovery, фенсинг, ретраи и классификация ошибок, дедуп событий
(через PK), outbox relay, durable-таймеры, admission control, планировщик, компенсация, идемпотентный
ключ публикации. Внешний сервис отвечает за собственную дедупликацию по ключу. Подробная таблица — README,
раздел «Гарантии инфраструктуры vs кода приложения».

## 8. Ограничения и честные оговорки

* **Real LLM: два небольших прогона.** Прогон с реальной LLM выполнен локально на модели `qwen3.5:4b` (Ollama, CPU) с параметром `reasoning_effort=none`: без него модель тратит токены на рассуждение и возвращает пустой `content`. Выполнены 3 job и по одному повтору трёх сценариев восстановления (baseline, kill во время LLM-вызова, крэш после публикации), поэтому это иллюстрация, а не статистика. Таймаут ожидания воркера в сценарии восстановления увеличен с 60 до 300 с из-за медленной локальной модели. Две первые попытки завершились HTTP 503 (неверное имя модели в `.env`; затем 503 до отключения рассуждения, точную причину не установили); их результаты в отчёт не включены. Второй прогон (3 job и по одному повтору тех же сценариев) выполнен на сервере университета с моделью `qwen3.8-27b` (vLLM, доступ через VPN, токен хранится только в локальном `.env`) и опубликован отдельным блоком; стоимость не считалась, так как тариф сервера неизвестен. В обоих прогонах использовался `LLM_REASONING_EFFORT=none` (задан в `.env`); влияет ли этот параметр на серверную модель, не проверялось. Результаты двух моделей и Simulated не смешиваются. Цена в таблице условная (локальная модель, реальных затрат нет).
* **Один узел, один писатель.** SQLite сериализует все записи; масштабирование — несколько процессов на одной
  машине (лимиты проверяются в транзакции и для них верны), но не кластер. Узкое место — частота транзакций
  (claim + complete на каждый шаг + heartbeats).
* **Основная ОС — Windows, Linux — контейнер на той же машине.** Основные таблицы Part 1–3 получены на Windows 10
  («SIGKILL» там — `Popen.kill()` → `TerminateProcess`, без обработчиков/`finally`). Все три части и автотесты
  повторены в Linux-контейнере с настоящим POSIX SIGKILL (`results/raw_linux/`, отдельные блоки «на Linux»), но это
  Docker Desktop с ядром WSL2 на том же железе, а не отдельный Linux-сервер; на другой машине не проверялось.
* **`synchronous=NORMAL`**: гарантия durable — против падения процесса (это и проверялось), но не против
  потери питания/краха ОС для последних транзакций (`AGENTRT_SYNC=FULL` закрывает это ценой латентности; не измерялось).
* **Масштабированное время** в Part 3 (1 вирт. с = 0.1 с): накладные расходы рантайма (транзакции, поллинг)
  не масштабируются и в вирт. секундах выглядят больше (оценка приведена). Абсолютные значения — для этой
  машины и этого масштаба; сравнение политик корректно, т.к. условия одинаковые.
* **Статистика.** Part 1: 5 повторов на сценарий; Part 3: 3 прогона на политику — разброс p95 по seed показан,
  доверительные интервалы не считались. Для `light*`×`long` число наблюдений невелико (см. N в таблице).
* **Неопределённость внешних вызовов** не устранима на стороне клиента: прерванный LLM-вызов может быть
  оплачен; мы даём верхнюю оценку (prompt + max_output_tokens), а не точное значение.
* **Окно публикации при cancel.** Отмена во время `publish` не может «отозвать» уже отправленный запрос;
  инвариант «нет живой публикации у не-SUCCEEDED job» конечный (eventual): публикация существует до отзыва
  (до закрытия run и следующего прохода relay; при падении воркера — до истечения lease). Долгая недоступность
  сервиса публикаций не проверялась: отзыв повторяется без ограничения числа попыток.
* **Мгновенное восстановление по имени воркера** (`recover_own`) предполагает, что прежний процесс с тем же
  `--name` мёртв; это не проверяется (нет проверки pid). Два живых воркера с одним именем — ошибка конфигурации:
  фенсинг по `lease_token` не даст закоммитить чужой результат, но шаг (и LLM-вызов) может выполниться дважды.
* **Нагрузочный тест без approval** (`require_approval = false`): то, что ожидание approval не занимает слоты,
  показано автотестом и E2/E6, а не под нагрузкой Part 3.
* **Цены токенов** в конфиге иллюстративные (не прайс-лист провайдера); стоимость = токены × цена из конфига.
* **Межпроцессных уведомлений нет**: воркер узнаёт о новых событиях/заявках из других процессов поллингом
  (`poll_interval_s`), это входит в латентность «event → next step».
* **Outbox без dead-letter.** Недоставленные уведомления повторяются с ограниченной паузой (≤ 8 с), но без
  предела числа попыток; при долго недоступном получателе строки копятся в `outbox` (видны по `attempts`,
  `last_error`), отдельной очереди «мёртвых» сообщений нет. На состояние job это не влияет.
* **`docker-compose.yml` проверен сквозным сценарием**, а не нагрузкой: `scripts/demo_docker.ps1` поднимает publisher,
  api и worker, через HTTP API проверяет идемпотентное создание, SIGKILL воркера (`docker compose kill`) с
  автоматическим продолжением, дубль события и пересоздание всех контейнеров во время ожидания approval (состояние,
  дедлайн и `approval_id` сохраняются в именованном томе). Отчётные эксперименты в compose не запускались.
* **Воспроизводимость с нуля проверена** на той же машине: клон из Git → новое окружение из `requirements.txt` →
  `scripts/run_all_experiments.ps1` (≈ 14 мин) — тесты зелёные, выводы всех трёх частей совпали; абсолютные
  времена и доли исходов E5 отличаются (см. E5). В другой ОС/на другом железе не проверялось.
* Политика агента (`decide`) — детерминированные правила с ограничениями (min_docs, max_tool_calls, approval),
  а не решение LLM; LLM используется для черновика.

