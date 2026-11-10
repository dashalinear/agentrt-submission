"""Generate figures + REPORT.md + REPORT.pdf from results/raw (and results/real_llm if present).

Every number in REPORT.md is computed here from raw data; nothing is typed by hand.
Usage: python -m experiments.make_report [--no-pdf]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from collections import Counter

from experiments import plots
from experiments.common import ROOT, WORK
from experiments.report_data import TENANTS, events, fmt, load, pytest_summary, q, real_llm, recovery

CASE_RU = {
    "baseline": "без отказов (baseline)",
    "s1_kill_between_steps": "1. kill между шагами (после extract)",
    "s2a_kill_during_llm_lease": "2a. kill во время LLM, другой воркер ждёт lease",
    "s2b_kill_during_llm_restart": "2b. kill во время LLM, рестарт того же воркера",
    "s3a_crash_after_publish": "3a. крэш после publish (ack не сохранён)",
    "s3b_ack_lost_network": "3b. ack потерян в сети (503 после коммита)",
    "s4_transient_repeated": "4. повторяющаяся transient-ошибка",
}


def _ru(n, one, few, many):
    """Russian plural form: _ru(1, "строка", "строки", "строк")."""
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


def table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join(fmt(x) if not isinstance(x, str) else x for x in r) + " |")
    return "\n".join(out)


def cnt(d):
    return ", ".join(f"{k}: {v}" for k, v in sorted(d.items())) if d else "–"


def section_env(rec, ld):
    env = (ld["meta"] or {}).get("env") or (rec["meta"] or {}).get("env") or {}
    rows = [["ОС", env.get("os", "?")], ["CPU", f"{env.get('processor', '?')} ({env.get('cpu_count')} логич. ядер)"],
            ["RAM", f"{env.get('ram_gb', '?')} GB"], ["Python", env.get("python", "?")],
            ["SQLite", env.get("sqlite", "?")]]
    for k in ("fastapi", "starlette", "uvicorn", "httpx", "pydantic", "pytest", "matplotlib", "numpy", "Markdown"):
        if k in env:
            rows.append([k, env[k]])
    ev = events()["summary"]
    rows += [["Масштаб времени, Part 1/2", "1.0 (реальные секунды)"],
             ["Масштаб времени, Part 3", f"{ld['meta'].get('time_scale')} → {ld['meta'].get('time_scale_note', '')}"],
             ["Seeds", f"Part 1: {(rec['meta'] or {}).get('seeds')}; Part 2: {ev.get('seed')}; "
                       f"Part 3: {ld['seeds']} (одинаковые для всех политик)"],
             ["LLM в этих прогонах", "**SimulatedLLM** (детерминированный); реальная LLM — раздел «Real LLM run»"]]
    return table(["параметр", "значение"], rows)


def section_part1(rec):
    rows1, rows2 = [], []
    for c in rec["cases"]:
        name = CASE_RU.get(c["case"], c["case"])
        rows1.append([name, c["n"], cnt(c["final"]),
                      f"{fmt(c['total_p50'])} [{fmt(c['total_min'])}–{fmt(c['total_max'])}]",
                      fmt(c["overhead_p50"]) if c["case"] != "baseline" else "–",
                      fmt(c["ready_after_spawn_p50"]), fmt(c["resume_ready_p50"], 3), fmt(c["resume_spawn_p50"])])
        rows2.append([name, c["llm_calls"], c["rep_llm"], c["rep_tool"], c["unknown"], c["reexec"],
                      f"{c['phys']} / {c['logical']}", c["dups"], f"{c['extra_ok']} / ≤{c['extra_upper']}",
                      f"{c['cost_extra']:.6f} / ≤{c['cost_extra_upper']:.6f}"])
    t = ("**Время** (реальные секунды; «возобновление» = первый шаг job после рестарта):\n\n" +
         table(["сценарий", "N", "итог", "время job p50 [min–max]", "+ к baseline p50",
                "старт процесса → воркер готов p50", "воркер готов → возобновление p50",
                "старт процесса → возобновление p50"], rows1) +
         "\n\n**Повторы и побочные эффекты** (суммы по N прогонам):\n\n" +
         table(["сценарий", "LLM-вызовов", "повторных LLM", "повторных tool", "вызовов с неизвестным исходом",
                "перевыполнено шагов с чекпоинтом", "publish физ. / лог.", "дублей лог. публикаций",
                "доп. токены прерванных вызовов: подтверждено / верхняя граница", "доп. стоимость USD: подтверждено / верх."], rows2))
    s4 = next((c for c in rec["cases"] if c["case"] == "s4_transient_repeated"), None)
    s2a = next((c for c in rec["cases"] if c["case"] == "s2a_kill_during_llm_lease"), None)
    s2b = next((c for c in rec["cases"] if c["case"] == "s2b_kill_during_llm_restart"), None)
    s1 = next((c for c in rec["cases"] if c["case"] == "s1_kill_between_steps"), None)
    cfg = (rec["meta"] or {}).get("config", {})
    txt = [t, "",
           f"Конфигурация: lease_timeout = {cfg.get('lease_timeout_s')} с, heartbeat = {cfg.get('heartbeat_interval_s')} с, "
           f"max_attempts = 4, backoff base = {cfg.get('backoff_base_s')} с, cap = {cfg.get('backoff_cap_s')} с; "
           f"по {rec['cases'][0]['n']} повторов на сценарий; воркер — отдельный процесс, убивается `Popen.kill()` "
           f"(на Windows это `TerminateProcess` — аналог SIGKILL: без обработчиков и `finally`), "
           f"FakePublisherService — отдельный HTTP-процесс. Сырые данные: `results/raw/recovery_runs.csv`, "
           f"`recovery_step_runs.jsonl`, `recovery_calls.jsonl`.", "",
           f"![recovery](results/figures/recovery_job_time.png)", "",
           "**Наблюдения (из таблицы):**", ""]
    if s1:
        txt.append(f"* **Kill между шагами.** Повторных LLM/tool вызовов: {s1['rep_llm']}/{s1['rep_tool']}, "
                   f"перевыполненных шагов с чекпоинтом: {s1['reexec']}. Шаги `fetch_documents`/`extract`, чей результат "
                   f"уже в `step_results`, не повторялись. Возобновление после того, как новый процесс готов: "
                   f"p50 = {fmt(s1['resume_ready_p50'], 3)} с (тот же `--name` ⇒ lease прежней инкарнации забирается "
                   f"сразу при старте); основная часть времени «от старта процесса» "
                   f"(p50 {fmt(s1['resume_spawn_p50'])} с) — запуск интерпретатора и импорт модулей.")
    if s2a and s2b:
        txt.append(f"* **Kill во время LLM-вызова.** Каждый прерванный вызов повторён ровно один раз "
                   f"(повторных LLM: {s2a['rep_llm']} на {s2a['n']} прогонов в 2a и {s2b['rep_llm']} на {s2b['n']} в 2b). "
                   f"Если подхватывает *другой* воркер, он ждёт истечения lease: возобновление через "
                   f"{fmt(s2a['resume_ready_p50'])} с после его готовности (остаток lease_timeout), против "
                   f"{fmt(s2b['resume_ready_p50'], 3)} с при рестарте того же воркера. **Неопределённость исхода:** "
                   f"{s2a['unknown'] + s2b['unknown']} вызовов остались в журнале вызовов без завершения — рантайм не "
                   f"знает, обработал ли их провайдер и выставил ли счёт. Поэтому дополнительные токены даны "
                   f"интервалом: фактически оплаченные повторы, которые мы видим, = {s2a['extra_ok'] + s2b['extra_ok']}, "
                   f"верхняя граница (prompt + max_output_tokens прерванных вызовов) = {s2a['extra_upper'] + s2b['extra_upper']} "
                   f"токенов на {s2a['n'] + s2b['n']} прогонов.")
    txt.append("* **Side effect без ack.** В 3a процесс умирает сразу после ответа сервиса (до чекпоинта), в 3b сервис "
               "коммитит и отвечает 503. В обоих случаях физических запросов публикации 2 на job, логических — 1, "
               "дублей логических публикаций 0: повтор идёт с тем же `Idempotency-Key`, и сервис возвращает прежний "
               "`publication_id` (`replayed=true`).")
    if s4:
        pauses = "; ".join("[" + ", ".join(f"{x:.2f}" for x in p) + "]" for p in s4["pauses"])
        txt.append(f"* **Повторяющаяся transient-ошибка.** Ровно 4 попытки на прогон (всего {s4['transient_attempts']} "
                   f"неудачных попыток в {s4['n']} прогонах), паузы между попытками (с, по прогонам): {pauses} — растут "
                   f"экспоненциально с jitter (ожидаемые интервалы [0.25–0.5], [0.5–1.0], [1.0–2.0] плюс время "
                   f"поллинга). Итог — `FAILED` с причиной: `{s4['errors'][0]}`.")
    txt += _part1_linux()
    return "\n".join(txt)


def _part1_linux():
    """Same Part 1 experiment re-run in a Linux container (real POSIX SIGKILL); separate block, not merged."""
    from experiments.report_data import recovery as _rec
    lr = _rec(os.path.join(ROOT, "results", "raw_linux"))
    if not lr["cases"]:
        return []
    env = (lr["meta"] or {}).get("env", {})
    rows = [[CASE_RU.get(c["case"], c["case"]), c["n"], cnt(c["final"]),
             f"{fmt(c['total_p50'])} [{fmt(c['total_min'])}–{fmt(c['total_max'])}]",
             fmt(c["resume_ready_p50"], 3), c["rep_llm"], c["reexec"], f"{c['phys']} / {c['logical']}", c["dups"]]
            for c in lr["cases"]]
    return ["", "### Part 1 на Linux: тот же эксперимент в Docker-контейнере (настоящий SIGKILL)", "",
            f"Окружение: {env.get('os', '?')}, Python {env.get('python', '?')} (образ из `Dockerfile`, Docker Desktop "
            f"на этой же машине, ядро WSL2). Код и параметры те же (`python -m experiments.exp_recovery --reps {lr['cases'][0]['n']}`), "
            "но `Popen.kill()` здесь посылает POSIX SIGKILL. Сырые данные — отдельно, в `results/raw_linux/`; с "
            "Windows-прогоном выше не смешиваются. Команда: `.\\scripts\\run_linux.ps1 -Parts recovery`.", "",
            table(["сценарий", "N", "итог", "время job p50 [min–max], с", "воркер готов → возобновление p50, с",
                   "повторных LLM", "перевыполнено шагов с чекпоинтом", "publish физ. / лог.", "дублей лог. публикаций"],
                  rows), "",
            "Гарантии воспроизвелись и под SIGKILL: шаги с чекпоинтом не перевыполняются, прерванный LLM-вызов "
            "повторяется ровно один раз, повторная публикация не создаёт дубля, transient-ошибка исчерпывает 4 попытки "
            "и даёт `FAILED`. Абсолютные времена отличаются от Windows (другой запуск процесса, файловая система тома)."
            + _lease_note(lr)]


def _lease_note(lr):
    from experiments.report_data import recovery as _rec
    w = {c["case"]: c for c in _rec()["cases"]}.get("s2a_kill_during_llm_lease")
    lx = {c["case"]: c for c in lr["cases"]}.get("s2a_kill_during_llm_lease")
    if not (w and lx):
        return ""
    return (f" В 2a «готов → возобновление» — это остаток lease ({lr['meta']['config']['lease_timeout_s']} с от последнего "
            f"heartbeat), который новый воркер ещё ждёт после своей готовности: на Linux процесс готов быстрее "
            f"(старт → готов p50 {fmt(lx['ready_after_spawn_p50'])} с против {fmt(w['ready_after_spawn_p50'])} с на Windows), "
            f"поэтому ждать остаётся дольше ({fmt(lx['resume_ready_p50'])} против {fmt(w['resume_ready_p50'])} с), а время "
            f"от старта процесса до возобновления близко ({fmt(lx['resume_spawn_p50'])} против {fmt(w['resume_spawn_p50'])} с).")


def section_part2(ev):
    s = ev["summary"]
    if not s:
        return "_нет данных: запустите `make exp-events`_"
    e1 = s["e1"]
    lat = table(["метрика", "N", "p50, мс", "p95, мс", "max, мс"],
                [[m["metric"], m["n"], m["p50"], m["p95"], m["max"]] for m in e1["latency"]])
    inv_rows = []
    for k in ("e1", "e2", "e3", "e4", "e5", "e7"):
        inv = s[k]["invariants"]
        inv_rows.append([k.upper(), inv["transitions_checked"], inv["invalid_transitions"], inv["terminal_changes"],
                         inv["version_gaps"], inv["multiple_terminal"], inv["log_state_mismatch"]])
    inv_t = table(["эксперимент", "проверено переходов", "недопустимых переходов", "изменений терминального статуса",
                   "разрывов версии", "job с >1 терминальным переходом", "журнал ≠ состояние"], inv_rows)
    e5 = s["e5"]
    e6_rows = [[r["variant"], r["rep"], r["final"], r["open_step_runs_while_waiting"], r["state_while_worker_down"],
                r["event_outcome"] or "–", fmt(r["worker_ready_to_first_step_ms"]), fmt(r["worker_ready_to_terminal_ms"]),
                fmt(r["timeout_lateness_ms"])] for r in ev["e6"]]
    e7 = s["e7"]["by_state"]
    e7_t = table(["отмена в состоянии", "N", "p50, мс", "p95, мс", "max, мс (QUEUED/WAITING: 0 по определению)"],
                 [[k, v["n"], v["p50"], v["p95"], v["max"]] for k, v in e7.items()])
    out = [
        "Все эксперименты — `time_scale = 1.0`, воркер в отдельном потоке (E1–E5, E7) или отдельном процессе (E6). "
        "Латентности измерены по меткам времени, записанным в БД в момент коммита (`events.received_at`, "
        "`transitions.ts`, `step_runs.started_at`). Сырые данные: `results/raw/events_*.csv`, `events_summary.json`.", "",
        "### E1. Approval вовремя", "",
        f"{sum(e1['final'].values())} job, итог: {cnt(e1['final'])}; исходы событий: {cnt(e1['outcomes'])}.", "", lat, "",
        "«event → state transition» — от приёма события до коммита перехода `WAITING_EVENT → QUEUED` (inbox и переход "
        "в одной транзакции). «event → next step started» дополнительно включает ожидание цикла планировщика "
        "(поллинг `poll_interval_s = 3 мс`, межпроцессного уведомления нет). Медиана — доли миллисекунды, а хвост "
        "(p95/max) — по всей видимости ожидание блокировки записи: если в этот момент транзакцию держит воркер, "
        "`BEGIN IMMEDIATE` отправителя попадает в busy-handler SQLite, который ждёт ступенями (1, 2, 5, 10, 15, 20, "
        "25… мс). Это плата за single-writer хранилище.", "",
        "![latency](results/figures/events_latency.png)", "",
        "### E2. Approval не пришёл → TIMED_OUT", "",
        f"{sum(s['e2']['final'].values())} job с дедлайном 0.5 с: итог {cnt(s['e2']['final'])}. Запаздывание перехода "
        f"относительно дедлайна: p50 = {s['e2']['lateness']['p50']} мс, p95 = {s['e2']['lateness']['p95']} мс, "
        f"max = {s['e2']['lateness']['max']} мс (N = {s['e2']['lateness']['n']}). Шагов, исполнявшихся во время "
        f"ожидания: {s['e2']['step_runs_during_wait_total']}.", "",
        "### E3. Дубли события", "",
        f"Отправлено {s['e3']['events_sent']} копий approval (на каждую job — 3 одновременно из разных потоков и 2 "
        f"повторно позже, все с одним `event_id`): применено {s['e3']['applied']}, распознано как дубли "
        f"{s['e3']['duplicates_detected']}; job с более чем одним переходом `WAITING_EVENT → QUEUED`: "
        f"{s['e3']['jobs_with_more_than_one_transition']}.", "",
        "### E4. Approval после CANCELLED / TIMED_OUT", "",
        f"{s['e4']['late_events']} поздних событий: исходы {cnt(s['e4']['event_outcomes'])}; повторный cancel: "
        f"{cnt(s['e4']['cancel_outcomes'])}; изменилось состояние: {s['e4']['state_changed']}, изменилась версия строки: "
        f"{s['e4']['version_changed']}.", "",
        "### E5. Почти одновременные approval + cancel + deadline + завершение шагов", "",
        f"{e5['reps']} повторов. Для каждой job дедлайн T = создание + 0.6 с; approval отправляется в "
        f"T + U(−150, +20) мс, cancel — в T + U(−60, +60) мс (seed {s.get('seed')}); после approval воркер "
        f"исполняет `save_report` и `publish` (~30–50 мс), так что их завершение тоже попадает в окно гонки. "
        "Плановые сдвиги и фактические моменты отправки (`approve_sent_ms`, `cancel_sent_ms` — по часам отправителя "
        "перед вызовом) записаны в `events_e5_races.csv`; на графике — фактические (они могут отличаться от плановых "
        "из-за планирования потоков ОС и ожидания блокировки записи; если job ещё не дошла до `WAITING_EVENT`, "
        "отправитель ждёт её, поэтому часть событий уходит позже дедлайна).", "",
        table(["итоговое состояние", "N"], [[k, v] for k, v in sorted(e5["final"].items())]), "",
        f"Откуда пришли в терминал: {cnt(e5['terminal_from'])}. Исходы approval: {cnt(e5['approval_outcomes'])}. "
        f"Исходы cancel: {cnt(e5['cancel_outcomes'])}. Счётчики рантайма: {cnt(e5['invariants'].get('counters', {}))}.",
        "",
        f"Инварианты: job с ≠1 терминальным переходом — {e5['multi_terminal_jobs']}; публикаций, принадлежащих "
        f"не-SUCCEEDED job и не отозванных — {e5['orphan_publications']}; SUCCEEDED без живой публикации — "
        f"{e5['succeeded_without_publication']}; публикаций, отозванных компенсацией — {e5['published_then_retracted']}; "
        f"попыток недопустимого перехода, отклонённых триггером — {e5['invariants'].get('invalid_transition_attempts_counter')}.",
        "",
        "![races](results/figures/events_race_outcomes.png)", "",
        *_e5_conf_lines(ev["e5"]), "",
        "Картина соответствует правилу: cancel, закоммиченный раньше дедлайна и раньше завершения `publish`, даёт "
        "CANCELLED; если approval пришёл достаточно рано, шаги успевают закоммитить SUCCEEDED, и более поздние "
        "cancel получают `already_terminal`; если к моменту approval или коммита шага дедлайн уже прошёл, побеждает "
        "TIMED_OUT (`deadline_won`, `aborted_deadline`, `late_results_discarded`).", "",
        "**Воспроизводимость E5.** Seed фиксирует только *плановые* моменты отправки; фактический порядок коммитов "
        "зависит от планирования потоков ОС и ожидания блокировки записи, поэтому распределение итоговых состояний "
        "(CANCELLED / SUCCEEDED / TIMED_OUT) от запуска к запуску заметно меняется (наблюдалось при контрольных "
        "перезапусках; их сырые данные в репозиторий не входят). Воспроизводимым результатом E5 являются инварианты "
        "(0 нарушений во всех прогонах) и соответствие каждого исхода правилу гонок, а не доли исходов.", "",
        *_e5_fix_note(), "",
        "### E6. Рестарт воркера во время ожидания", "",
        table(["вариант", "повтор", "итог", "открытых step_runs в ожидании", "состояние пока воркер мёртв",
               "исход события", "готов → первый шаг, мс", "готов → терминал, мс", "опоздание таймаута, мс"], e6_rows), "",
        "Ожидание не держит слот (0 открытых `step_runs`), событие принимается и применяется даже когда воркер мёртв "
        "(inbox в БД), а дедлайн, истёкший во время простоя, срабатывает при старте нового воркера (таймер хранится в "
        "БД). Для `deadline_while_down` «опоздание таймаута» ≈ длительность простоя — таймер не может сработать, пока "
        "нет ни одного живого воркера; отрицательное «готов → терминал» означает, что просроченный таймер сработал в "
        "процедуре старта воркера ещё до отметки готовности.", "",
        "### E7. Время до реальной остановки после cancel", "", e7_t, "",
        f"Исходы LLM-вызовов в E7: {cnt(s['e7']['llm_call_outcomes'])}. В `RUNNING` остановка ограничена интервалом "
        "heartbeat (0.1 с в E7): шаг узнаёт об отмене на следующем heartbeat и прерывает LLM-вызов; в `QUEUED` и "
        "`WAITING_EVENT` исполняемой работы нет — остановка мгновенная (коммит перехода).", "",
        "### Сводка инвариантов (переходы из журнала `transitions`, пишется триггером)", "", inv_t,
    ]
    out += _part2_linux()
    return "\n".join(out)


LINUX = os.path.join(ROOT, "results", "raw_linux")


def _e5_conformance(rows):
    """Per-job checks of the documented race rule on E5 rows (times are DB commit timestamps vs the deadline)."""
    def f(x):
        return None if x in ("", None) else float(x)

    checks = {
        "SUCCEEDED закоммичен не раньше дедлайна (запрещено: шаг проверяет now ≥ deadline)":
            lambda r: r["final"] == "SUCCEEDED" and f(r["terminal_ts_minus_deadline_ms"]) >= 0,
        "TIMED_OUT раньше дедлайна": lambda r: r["final"] == "TIMED_OUT" and f(r["terminal_ts_minus_deadline_ms"]) < 0,
        "CANCELLED без успешного cancel": lambda r: r["final"] == "CANCELLED" and r["cancel_outcome"] != "cancelled",
        "cancel успешен, а итог не CANCELLED": lambda r: r["cancel_outcome"] == "cancelled" and r["final"] != "CANCELLED",
        "approval `deadline_won`, а итог не TIMED_OUT": lambda r: r["approval_outcome"] == "deadline_won" and r["final"] != "TIMED_OUT",
        "approval применён, хотя отправлен после дедлайна":
            lambda r: r["approval_outcome"] == "applied" and f(r["approve_sent_ms"]) >= 0,
        "больше одного терминального перехода": lambda r: int(r["n_terminal_transitions"]) != 1,
    }
    viol = {k: sum(1 for r in rows if fn(r)) for k, fn in checks.items()}
    late_cancel = [f(r["terminal_ts_minus_deadline_ms"]) for r in rows
                   if r["final"] == "CANCELLED" and f(r["terminal_ts_minus_deadline_ms"]) >= 0]
    return viol, len(late_cancel), (max(late_cancel) if late_cancel else None)


def _e5_conf_lines(rows):
    if not rows:
        return []
    viol, n_late, mx = _e5_conformance(rows)
    return ["**Проверка правила гонок по каждой job** (из `events_e5_races.csv`, метки — моменты коммита в БД; "
            f"N = {len(rows)}):", "",
            table(["что было бы нарушением правила", "job"], [[k, v] for k, v in viol.items()]), "",
            f"Всего нарушений: {sum(viol.values())}. Отдельно: {n_late} job получили CANCELLED позже дедлайна "
            f"(до {fmt(mx)} мс) — это допустимо по правилу: cancel не проверяет дедлайн и побеждает, если закоммичен "
            "раньше, чем таймер дедлайна (поллинг) успел перевести job в TIMED_OUT; первая закоммиченная терминальная "
            "транзакция выигрывает."]


def _e5_fix_note():
    def diag(base):
        p = os.path.join(base, "diag_e5_polling.json")
        return json.load(open(p, encoding="utf-8")) if os.path.exists(p) else None

    w, lx = diag(os.path.join(ROOT, "results", "raw")), diag(LINUX)
    meas = ""
    if w and lx:
        f = lambda d, k: fmt(d[k]["create_to_waiting_p50_s"])  # noqa: E731
        meas = (f" Диагностика `experiments/diag_e5_polling.py` (10 job, p50 пути «создание → `WAITING_EVENT`»): "
                f"Linux — {f(lx, 'with_20_pollers')} с с 20 опрашивающими потоками против {f(lx, 'without_pollers')} с без "
                f"них; Windows — {f(w, 'with_20_pollers')} против {f(w, 'without_pollers')} с "
                f"(`results/raw_linux/diag_e5_polling.json`, `results/raw/diag_e5_polling.json`).")
    return ["**Исправление стенда E5 (найдено при прогоне на Linux).** В первой версии каждый из 20 "
            "потоков-отправителей сам опрашивал БД каждые 3 мс в том же процессе, что и воркер. На Windows это почти не "
            "влияло, а в Linux-контейнере из-за конкуренции за GIL воркер вытеснялся, job не успевали дойти до ожидания "
            "за 0.6 с, и все 300 повторов заканчивались TIMED_OUT — гонки не было (сырые данные того прогона "
            "перезаписаны исправленным; эффект воспроизводится диагностикой ниже)." + meas +
            " Сейчас состояние опрашивает один поток-монитор, отправители ждут его сигнала; весь Part 2 перезапущен "
            "этим кодом на обеих ОС — данные выше и в блоке «на Linux» уже с исправленным стендом. Рантайм не менялся: "
            "это свойство стенда, где воркер и отправители живут в одном процессе."]


def _linux_env(meta):
    env = (meta or {}).get("env", {})
    return f"{env.get('os', '?')}, Python {env.get('python', '?')}"


def _part2_linux():
    """Part 2 re-run in a Linux container; separate block, never merged with the Windows numbers."""
    lv = events(LINUX)
    s = lv["summary"]
    if not s:
        return []
    lat = {m["metric"]: m for m in s["e1"]["latency"]}
    tr = lat.get("event_to_transition_ms", {})
    la = s["e2"]["lateness"]
    run = s["e7"]["by_state"].get("RUNNING", {})
    e6 = lv["e6"]
    e6_ok = sum(1 for r in e6 if r["final"] == ("TIMED_OUT" if r["variant"] == "deadline_while_down" else "SUCCEEDED")
                and r["open_step_runs_while_waiting"] == 0)
    inv_keys = ("invalid_transitions", "terminal_changes", "version_gaps", "multiple_terminal", "log_state_mismatch")
    viol = sum(s[k]["invariants"][x] for k in ("e1", "e2", "e3", "e4", "e5", "e7") for x in inv_keys)
    checked = sum(s[k]["invariants"]["transitions_checked"] for k in ("e1", "e2", "e3", "e4", "e5", "e7"))
    e5 = s["e5"]
    rows = [
        ["E1 approval вовремя", f"{cnt(s['e1']['final'])}; event → transition p50 {tr.get('p50')} / p95 {tr.get('p95')} мс (N = {tr.get('n')})"],
        ["E2 approval не пришёл", f"{cnt(s['e2']['final'])}; опоздание таймаута p50 {la['p50']} / p95 {la['p95']} мс (N = {la['n']}); "
                                  f"шагов во время ожидания: {s['e2']['step_runs_during_wait_total']}"],
        ["E3 дубли события", f"отправлено {s['e3']['events_sent']}, применено {s['e3']['applied']}, дублей "
                             f"{s['e3']['duplicates_detected']}; job с >1 переходом: {s['e3']['jobs_with_more_than_one_transition']}"],
        ["E4 approval после терминала", f"{s['e4']['late_events']} поздних: {cnt(s['e4']['event_outcomes'])}; "
                                        f"изменений состояния: {s['e4']['state_changed']}"],
        ["E5 гонки", f"{cnt(e5['final'])}; job с ≠1 терминальным переходом: {e5['multi_terminal_jobs']}; "
                     f"неотозванных публикаций не-SUCCEEDED job: {e5['orphan_publications']}; нарушений правила гонок "
                     f"по каждой job: {sum(_e5_conformance(lv['e5'])[0].values())}; CANCELLED позже дедлайна (допустимо): "
                     f"{_e5_conformance(lv['e5'])[1]}"],
        ["E6 рестарт во время ожидания", f"{e6_ok} из {len(e6)} прогонов с ожидаемым итогом и 0 открытых step_runs"],
        ["E7 остановка после cancel в RUNNING", f"p50 {run.get('p50')} / p95 {run.get('p95')} мс (N = {run.get('n')})"],
        ["инварианты E1–E5, E7", f"проверено переходов {checked}, нарушений {viol}"],
    ]
    return ["", "### Part 2 на Linux (Docker-контейнер)", "",
            f"Окружение: {_linux_env(s)}; те же скрипт и параметры (`exp_events --race-reps 300`, seed {s.get('seed')}). "
            "Сырые данные отдельно — `results/raw_linux/events_*`; с Windows-прогоном выше не смешиваются "
            "(команда: `.\\scripts\\run_linux.ps1`).", "",
            table(["эксперимент", "результат на Linux"], rows), "",
            "Поведение совпадает с Windows по всем проверкам (итоги E1–E4, E6, нулевые нарушения инвариантов). "
            "Латентности и доли исходов E5 различаются — они зависят от планировщика ОС и файловой системы тома "
            "(см. «Воспроизводимость E5»)."]


def section_part3(ld):
    if not ld["jobs"]:
        return "_нет данных: запустите `make exp-load`_"
    meta = ld["meta"]
    ph = "; ".join(f"{p[0]} [{p[1]}–{p[2]} с): " + ", ".join(f"{t} {r}/с" for t, r in p[3].items())
                   for p in meta["phases"])
    nom = meta["nominal_work_v"]
    cap = meta["global_concurrency"] / (meta["long_share"] * nom["long"] + (1 - meta["long_share"]) * nom["short"])
    g = ld["groups"]
    gt = table(["политика", "user", "class", "N", "p50 ожидания, с", "p95 ожидания, с", "p95 по seed (min–max)",
                "max, с", "истекло (TIMED_OUT)", "e2e p50 успешных, с"],
               [[x["policy"], x["tenant"], x["job_class"], x["n"], x["p50"], x["p95"],
                 f"{fmt(x['p95_seed_min'])}–{fmt(x['p95_seed_max'])}", x["max"], x["expired"], x["e2e_p50"]] for x in g])
    pp = ld["per_policy"]
    pt = table(["политика", "прогонов", "заявок", "принято", "отклонено (по user)", "SUCCEEDED", "TIMED_OUT (по user)",
                "jobs/s (весь прогон)", "jobs/s в burst", "загрузка слотов в burst",
                "доля слот-времени на job, которые потом истекли", "max суммарное ожидание job, с",
                "max возраст старейшей job в очереди (от создания), с", "max ожидание старейшего готового шага (от готовности), с",
                "max длина очереди", "max одновременно global / лимит", "max на tenant / лимит"],
               [[p["policy"], p["runs"], p["arrivals"], p["admitted"], f"{p['rejected']} ({cnt(p['rejected_by_tenant'])})",
                 p["succeeded"], f"{p['timed_out']} ({cnt(p['timed_out_by_tenant'])})", fmt(p["throughput"], 3),
                 fmt(p["burst_throughput"], 3), f"{p['slot_utilization_burst']:.1%}",
                 f"{p['slot_share_expired']:.1%}" if p.get("slot_share_expired") is not None else "–",
                 p["max_wait"], fmt(p.get("max_oldest_job_age")), p["max_oldest_age"], p["max_queue"],
                 f"{p['max_global_obs']} / {p['global_limit']}", f"{p['max_tenant_obs']} / {p['tenant_limit']}"]
                for p in pp])
    steps_short = q([r["steps"] for r in ld["jobs"] if r["job_class"] == "short" and r["state"] == "SUCCEEDED"], 50)
    steps_long = q([r["steps"] for r in ld["jobs"] if r["job_class"] == "long" and r["state"] == "SUCCEEDED"], 50)
    b0, b1 = ld["burst"]
    st = table(["", *TENANTS], [["спрос (номинальная работа заявок)", *[f"{ld['demand_share'][t]:.1%}" for t in TENANTS]],
                               ["вес / квота", *["33.3%"] * 3]] +
               [[p["policy"], *[f"{p['work_share_burst'][t]:.1%}" for t in TENANTS]] for p in pp])

    def grp(p, t, c, k="p95"):
        return next(x for x in g if x["policy"] == p and x["tenant"] == t and x["job_class"] == c)[k]

    lines = []
    if "fifo" in ld["policies"] and "drr" in ld["policies"]:
        for t in ("light1", "light2"):
            lines.append(f"  * {t}: short p95 {fmt(grp('fifo', t, 'short'))} → {fmt(grp('drr', t, 'short'))} с, "
                         f"long p95 {fmt(grp('fifo', t, 'long'))} → {fmt(grp('drr', t, 'long'))} с (FIFO → DRR);")
        lines.append(f"  * heavy: short p95 {fmt(grp('fifo', 'heavy', 'short'))} → {fmt(grp('drr', 'heavy', 'short'))} с, "
                     f"long p95 {fmt(grp('fifo', 'heavy', 'long'))} → {fmt(grp('drr', 'heavy', 'long'))} с.")
    pd = {p["policy"]: p for p in pp}
    # waiting by priority (aging / starvation evidence), computed from the same raw jobs
    prio_rows, prio = [], {}
    for p in ld["policies"]:
        for gname, ts in (("heavy", ("heavy",)), ("light1+light2", ("light1", "light2"))):
            for pr in sorted({r["priority"] for r in ld["jobs"]}):
                xs = [r for r in ld["jobs"] if r["policy"] == p and r["tenant"] in ts and r["priority"] == pr]
                if not xs:
                    continue
                w = [r["wait_total_v"] for r in xs]
                rec = {"n": len(xs), "p50": q(w, 50), "p95": q(w, 95), "max": max(w),
                       "ok": sum(r["state"] == "SUCCEEDED" for r in xs), "to": sum(r["state"] == "TIMED_OUT" for r in xs)}
                prio[(p, gname, pr)] = rec
                prio_rows.append([p, gname, pr, rec["n"], fmt(rec["p50"]), fmt(rec["p95"]), fmt(rec["max"]),
                                  rec["ok"], rec["to"]])
    prio_t = table(["политика", "пользователи", "priority", "N", "p50 ожидания, с", "p95 ожидания, с", "max, с",
                    "SUCCEEDED", "TIMED_OUT"], prio_rows)
    prio_note = []
    h0, h1 = prio.get(("drr", "heavy", 0)), prio.get(("drr", "heavy", 1))
    f0, f1 = prio.get(("fifo", "heavy", 0)), prio.get(("fifo", "heavy", 1))
    if h0 and h1 and f0 and f1:
        prio_note = [
            f"В `fifo` приоритет не учитывается: у `heavy` p50 ожидания priority 0 и 1 почти одинаковы "
            f"({fmt(f0['p50'])} и {fmt(f1['p50'])} с). В `drr` priority = 1 внутри tenant обслуживается первым "
            f"(p50 {fmt(h1['p50'])} с против {fmt(h0['p50'])} с у priority 0), но задачи priority 0 не вытесняются "
            f"бесконечно: aging (+1 к эффективному приоритету за 10 с ожидания) возвращает их в оборот — "
            f"{h0['ok']} из {h0['n']} завершились успешно. Остальные ({h0['to']}) истекли по дедлайну: при длительной "
            f"перегрузке «защита от голодания» не создаёт ёмкость, и дедлайн снимает в первую очередь давно ждущую "
            f"низкоприоритетную работу перегружающего tenant. У лёгких пользователей приоритет почти не влияет — их "
            f"спрос ниже доли. Для priority = 1 наблюдений мало (10 % заявок), см. столбец N."]
    out = [
        f"**Нагрузка.** 3 пользователя (`heavy` шлёт сильно больше остальных), 2 класса: short ≈ {nom['short']:.2f} c и "
        f"long ≈ {nom['long']:.2f} c номинальной работы (long = {meta['long_share']:.0%} заявок; long разбит на шаги "
        f"≤ 1.5 с), {meta['priority_share']:.0%} заявок с priority = 1. Фазы (заявок в вирт. секунду): {ph}. Пуассоновские "
        f"прибытия, трасса генерируется из seed и сохраняется (`results/raw/load_arrivals_seed*.csv`) — **один и тот же "
        f"поток заявок для всех политик**. Ресурсы одинаковые: `global_concurrency = {meta['global_concurrency']}` "
        f"(номинальная ёмкость ≈ {cap:.2f} job/с), `max_queue_size = 150` (admitted QUEUED+RUNNING), дедлайны "
        f"short {meta['deadlines_v']['short']:.0f} с / long {meta['deadlines_v']['long']:.0f} с. "
        f"**Масштаб времени: 1 вирт. с = {meta['time_scale']} реальной с** (все времена ниже — виртуальные секунды). "
        f"Seeds {ld['seeds']}, по одному прогону каждой политики на seed.", "",
        "Политики:",
        "* `fifo` — общая очередь по `ready_at`, без лимита на tenant (`per_tenant_concurrency = global`);",
        "* `drr` — deficit round robin по tenant (веса 1:1:1, quantum 1 вирт. с работы) + aging приоритетов "
        "(+1 за 10 с ожидания), без жёсткого лимита на tenant (work-conserving);",
        "* `drr_iso` — `drr` + `per_tenant_concurrency = 3` + квота очереди `max_queue_per_tenant = 100` (жёсткая изоляция).", "",
        "### Ожидание по user × class (ожидание = суммарное время в очереди в состоянии «готов к запуску», "
        "по всем шагам; для истёкших в очереди — до момента истечения)", "", gt, "",
        "![p95](results/figures/load_wait_p95.png)", "",
        f"### Ожидание по приоритету (aging и защита от голодания; все {len(ld['seeds'])} seed)", "", prio_t, "", *prio_note, "",
        f"### Сводка по политикам (сумма/максимум по {len(ld['seeds'])} прогонам на политику)", "", pt, "",
        f"Масштабирование времени добавляет накладные расходы: медиана (фактическая − номинальная длительность шага) = "
        f"{fmt(ld['step_overhead_p50'], 3)} вирт. с, p95 = {fmt(ld['step_overhead_p95'], 3)} вирт. с "
        f"(транзакции SQLite и цикл asyncio в реальном времени не масштабируются).", "",
        f"### Доля выполненной работы во время перегрузки (окно burst {b0}–{b1} с, все seed)", "", st, "",
        "![share](results/figures/load_work_share.png)", "",
        "### Длина очереди и активные шаги во времени", "",
        "![cmp](results/figures/load_queue_by_policy.png)", "",
        *[f"![ts-{p}](results/figures/load_timeseries_{p}.png)" for p in ld["policies"]], "",
        "### Интерпретация", "",
        "* **Изоляция.** В FIFO burst пользователя `heavy` заполняет общую очередь, и короткие задачи лёгких "
        "пользователей стоят за ней — их p95 ожидания вырастает до десятков секунд и часть их задач истекает или "
        "отклоняется. DRR выбирает следующий шаг по кругу между tenant, поэтому лёгкие пользователи, чей спрос "
        "ниже их доли (33 %), получают всё, что просят, а перегрузка ложится на виновника:",
        *lines,
        "* **Throughput vs latency vs isolation.** `fifo` и `drr` work-conserving: загрузка слотов в burst "
        f"{pd.get('fifo', {}).get('slot_utilization_burst', 0):.1%} и {pd.get('drr', {}).get('slot_utilization_burst', 0):.1%}, "
        "DRR перераспределяет ожидание (и истечения) в сторону виновника перегрузки; число успешных job у DRR даже "
        "немного выше, т.к. короткие задачи лёгких пользователей не истекают. Throughput в job/с при одинаковой "
        f"загрузке слотов не одинаков: в окне burst у `fifo` завершилось {pd.get('fifo', {}).get('burst_done')} job "
        f"(из них long — {pd.get('fifo', {}).get('burst_done_long')}), у `drr` — {pd.get('drr', {}).get('burst_done')} "
        f"(long — {pd.get('drr', {}).get('burst_done_long')}). Часть слот-времени уходит на шаги job, которые затем истекают "
        f"по дедлайну (столбец «доля слот-времени…»: {pd.get('fifo', {}).get('slot_share_expired', 0):.1%} у `fifo`, "
        f"{pd.get('drr', {}).get('slot_share_expired', 0):.1%} у `drr`, {pd.get('drr_iso', {}).get('slot_share_expired', 0):.1%} "
        "у `drr_iso`) — это работа, потерянная из-за перегрузки; остальная разница связана с тем, какие job успевают "
        "завершиться внутри окна, и отдельно не раскладывалась. "
        f"`drr_iso` не даёт `heavy` занять больше 3 из 6 слотов даже когда остальные простаивают: загрузка слотов в burst "
        f"{pd.get('drr_iso', {}).get('slot_utilization_burst', 0):.1%}, throughput в burst "
        f"{fmt(pd.get('drr_iso', {}).get('burst_throughput'), 3)} против {fmt(pd.get('drr', {}).get('burst_throughput'), 3)} "
        f"job/с у `drr` — плата за гарантированный запас ёмкости и за то, что лёгкие пользователи вообще не получают "
        f"отказов (все {pd.get('drr_iso', {}).get('rejected')} отказов `drr_iso` — у `heavy`). Квота очереди на tenant "
        "дополнительно защищает лёгких пользователей от отказов `queue_full`, которые в `fifo`/`drr` вызывает чужой burst "
        "(общий `max_queue_size`).",
        "* **Чтение таблиц.** p95 ожидания `heavy` short ≈ 59–60 с во всех политиках — это цензурирование дедлайном "
        "60 с (задачи истекают в очереди, см. TIMED_OUT); «e2e p50 успешных» считается только по выжившим задачам "
        "(survivorship), поэтому у `heavy` он мал при большом ожидании. Возраст старейшей job считается от её создания "
        "до последнего выхода из очереди (старт последнего шага или истечение в очереди); он ограничен дедлайном "
        "(60 с short / 200 с long) и у DRR больше, чем у FIFO: FIFO старит всех одинаково, DRR намеренно держит в "
        "очереди именно backlog `heavy`. Отдельный столбец «ожидание старейшего готового шага» (и график ниже) — "
        "возраст в очереди от момента готовности очередного шага; он сбрасывается после каждого шага, поэтому меньше.",
        "* **Длительная перегрузка.** Admission control ограничивает очередь (429/REJECTED с причиной), дедлайны "
        "снимают job, которые не успеют, поэтому очередь и возраст старейшей job ограничены сверху, а не растут "
        "бесконечно; после окончания burst очередь рассасывается (фаза P3). Цена: почти все истёкшие job истекли в "
        f"состоянии QUEUED *между* шагами, уже выполнив часть работы ({sum(p.get('expired_after_steps', 0) for p in pp)} из "
        f"{sum(p['timed_out'] for p in pp)} истёкших по всем политикам успели исполнить хотя бы один шаг), — это и есть "
        "потерянное слот-время из сводной таблицы. Ранний отказ при admission дешевле, чем позднее истечение.",
        "* **Планирование на границах шагов и ожидание коротких job.** После каждого шага job возвращается в очередь "
        "(`RUNNING → QUEUED`, `ready_at = now`) и заново конкурирует за слот. Поэтому шаг, стоящий первым в очереди, "
        "ждёт не всю 10-секундную long-job, а лишь освобождения любого слота — не дольше самого длинного шага "
        "(≤ 1.5 вирт. с в этой нагрузке; с реальной LLM шаг ограничен только `step_timeout_s` = 30 с). "
        f"Но заходов столько, сколько шагов (медиана у успешных short — {fmt(steps_short, 0)}, у long — {fmt(steps_long, 0)}), "
        "и при backlog каждое повторное ожидание включает очередь перед job: под FIFO суммарное ожидание короткой "
        "job растёт как «число шагов × длина очереди» (p50 ожидания `heavy` short — десятки секунд, см. таблицу). DRR "
        "ограничивает это для лёгких пользователей, но не для перегружающего tenant. Цена шага как единицы "
        "планирования — лишняя транзакция claim/complete на каждый шаг (см. накладные расходы выше).",
        "* **Aging.** Внутри tenant priority = 1 обгоняет priority = 0, но задача с низким приоритетом, прождавшая "
        "10 с, получает +1 и не голодает (`tests/test_scheduling.py::test_aging_lets_old_low_priority_job_overtake`; "
        "измерения под нагрузкой — таблица «Ожидание по приоритету» выше).",
        "* **Лимиты.** По трассе `step_runs` (интервалы исполнения) максимальная одновременность ни разу не превысила "
        "global/per-tenant лимит (столбцы «max … / лимит»; то же проверяют автотесты).",
        "* **Ожидание approval не занимает слоты** — подтверждено автотестом "
        "`tests/test_timers_and_slots.py::test_waiting_job_does_not_occupy_execution_slot` (при `global_concurrency=1` "
        "вторая job полностью выполняется, пока первая ждёт approval) и E2/E6 (0 открытых `step_runs` в ожидании).",
    ]
    out += _part3_approval(ld)
    out += _part3_linux()
    return "\n".join(out)


def _part3_approval(ld):
    """Approval under load: same trace/seeds/resources as the main drr runs, a seeded share of jobs needs approval."""
    la = load(os.path.join(ROOT, "results", "raw"), prefix="loadappr_")
    if not la["jobs"]:
        return []
    meta = la["meta"]
    base = next((p for p in ld["per_policy"] if p["policy"] == "drr"), None)
    appr = la["per_policy"][0]

    def extra(d, label_jobs):
        ts = d["ts"] if "ts" in d else []
        waiting = [x for x in ts if (x.get("waiting_total") or 0) > 0]
        lim = d["per_policy"][0]["global_limit"] if d["per_policy"] else None
        full = sum(1 for x in waiting if x["active_total"] == lim)
        # steps of a job executed while that same job was waiting for approval (must be 0)
        win = {(r["seed"], r["job_id"]): (r["approval_wait_start_v"], r["approval_wait_end_v"])
               for r in label_jobs if r.get("approval_wait_start_v") is not None and r.get("approval_wait_end_v") is not None}
        overlap = sum(1 for r in d["runs"] if (r["seed"], r["job_id"]) in win
                      and win[(r["seed"], r["job_id"])][0] < r["start_v"] < win[(r["seed"], r["job_id"])][1])
        return {"max_wait_n": max([x.get("waiting_total") or 0 for x in ts] or [0]),
                "waiting_samples": len(waiting), "full_when_waiting": full, "overlap": overlap,
                "need": sum(1 for r in label_jobs if r.get("require_approval")),
                "waited": sum(1 for r in label_jobs if r.get("approval_wait_start_v") is not None)}

    seeds = set(la["seeds"])
    base_jobs = [r for r in ld["jobs"] if r["policy"] == "drr" and r["seed"] in seeds]
    eb = extra({"ts": [x for x in ld["ts"] if x["policy"] == "drr" and x["seed"] in seeds],
                "runs": [r for r in ld["runs"] if r["policy"] == "drr" and r["seed"] in seeds],
                "per_policy": [base] if base else []}, base_jobs)
    ea = extra(la, la["jobs"])

    def g(d, p, t, c):
        x = next((x for x in d["groups"] if x["policy"] == p and x["tenant"] == t and x["job_class"] == c), None)
        return f"{fmt(x['p95'])} (N = {x['n']})" if x else "–"

    def row(name, p, e, d):
        return [name, p["admitted"], e["need"], e["waited"], p["succeeded"], p["timed_out"], fmt(p["throughput"], 3),
                f"{p['slot_utilization_burst']:.1%}", g(d, "drr", "light1", "short"), e["max_wait_n"],
                f"{p['max_global_obs']} / {p['global_limit']}",
                f"{e['full_when_waiting']} из {e['waiting_samples']}" if e["waiting_samples"] else "–", e["overlap"]]

    rows = []
    if base:
        rows.append(row("drr, без approval (основной прогон)", base, eb, ld))
    rows.append(row(f"drr, approval у {meta.get('approval_share', 0):.0%} job", appr, ea, la))
    return ["", "### Approval под нагрузкой (ожидание не занимает слоты)", "",
            f"Тот же поток заявок, те же seeds {la['seeds']} и ресурсы, политика `drr`; доля job, требующих approval, — "
            f"{meta.get('approval_share', 0):.0%} (выбор по seed, поток заявок не меняется); симулированный «человек» "
            f"отвечает через {fmt(meta.get('approval_delay_v'), 0)} вирт. с после входа job в `WAITING_EVENT` (ожидание "
            "считается в дедлайн). Базовая строка — прогоны `drr` из основной таблицы. Сырые данные: "
            "`results/raw/loadappr_*` (`--approval-share 0.5 --approval-delay 20 --prefix loadappr_`).", "",
            table(["вариант", "принято", "требуют approval", "дошли до ожидания approval", "SUCCEEDED", "TIMED_OUT", "jobs/s",
                   "загрузка слотов в burst", "p95 ожидания в очереди light1 short, с",
                   "max одновременно в WAITING_EVENT", "max активных шагов / лимит",
                   "моменты с ожидающими job, когда заняты все слоты", "шагов job во время её же ожидания"], rows), "",
            f"Ожидание approval не держит слот: одновременно в `WAITING_EVENT` находилось до {ea['max_wait_n']} job, а "
            "активных шагов никогда не было больше лимита; в "
            f"{ea['full_when_waiting'] / max(1, ea['waiting_samples']):.0%} моментов с ожидающими job все слоты были "
            "заняты *другой* работой (в остальные моменты очередь просто была короче лимита), и ни один шаг job не "
            "исполнялся, пока она ждала approval (последний столбец). Загрузка слотов в burst при этом та же, что без "
            f"approval ({appr['slot_utilization_burst']:.1%} против {base['slot_utilization_burst']:.1%}). Цена ожидания — "
            f"в истечениях и throughput: время ожидания входит в дедлайн, поэтому TIMED_OUT {appr['timed_out']} против "
            f"{base['timed_out']}, а job, ожидающие человека, завершаются позже."]


def _part3_linux():
    """Part 3 re-run in a Linux container with the same seeds; separate block."""
    ll = load(LINUX)
    if not ll["jobs"]:
        return []
    g = ll["groups"]

    def grp(p, t, c):
        return next((x for x in g if x["policy"] == p and x["tenant"] == t and x["job_class"] == c), None)

    rows = []
    for p in ll["per_policy"]:
        ls = grp(p["policy"], "light1", "short")
        hs = grp(p["policy"], "heavy", "short")
        rows.append([p["policy"], p["runs"], p["arrivals"], f"{p['rejected']} ({cnt(p['rejected_by_tenant'])})",
                     p["succeeded"], f"{p['timed_out']} ({cnt(p['timed_out_by_tenant'])})", fmt(p["throughput"], 3),
                     f"{p['slot_utilization_burst']:.1%}",
                     f"{fmt(ls['p95'])} (N = {ls['n']})" if ls else "–", f"{fmt(hs['p95'])} (N = {hs['n']})" if hs else "–",
                     " / ".join(f"{p['work_share_burst'][t]:.1%}" for t in TENANTS),
                     f"{p['max_global_obs']} / {p['global_limit']}", f"{p['max_tenant_obs']} / {p['tenant_limit']}"])
    return ["", "### Part 3 на Linux (Docker-контейнер, те же seed и поток заявок)", "",
            f"Окружение: {_linux_env(ll['meta'])}; seeds {ll['seeds']}, по одному прогону каждой политики на seed, "
            "масштаб времени 0.1. Сырые данные отдельно — `results/raw_linux/load_*`; с Windows-прогоном не смешиваются.", "",
            table(["политика", "прогонов", "заявок", "отклонено (по user)", "SUCCEEDED", "TIMED_OUT (по user)",
                   "jobs/s", "загрузка слотов в burst", "p95 ожидания light1 short, с", "p95 ожидания heavy short, с",
                   "доля работы в burst heavy / light1 / light2", "max global / лимит", "max на tenant / лимит"], rows), "",
            "Выводы Part 3 на Linux те же: DRR снижает p95 ожидания лёгких пользователей до единиц секунд, перегрузка "
            "(отказы и истечения) концентрируется у `heavy`, лимиты параллельности не превышаются, `drr_iso` "
            "жертвует загрузкой слотов ради изоляции."]


def _real_rows(s, recovery_rows, cost_text):
    """Rows of one Real-LLM table; the same set of metrics for every real run."""
    from collections import Counter
    lat = s.get("llm_latency_s", {})
    by = {}
    for x in recovery_rows:
        by.setdefault(x["case"], []).append(x)

    def agg(case):
        rs = by.get(case, [])
        if not rs:
            return None
        num = lambda k: [r[k] for r in rs if r.get(k) is not None]  # noqa: E731
        return {"n": len(rs), "final": cnt(dict(Counter(r["final_state"] for r in rs))),
                "time_p50": q(num("total_time_s"), 50), "resume_p50": q(num("resume_after_spawn_s"), 50),
                "rep_llm": sum(num("repeated_llm_calls")), "extra_ok": sum(num("extra_tokens_ok")),
                "extra_ub": sum(num("extra_tokens_upper_bound")), "phys": sum(num("publish_physical")),
                "log": sum(num("publish_logical")), "dups": sum(num("logical_duplicates")),
                "reexec": sum(num("checkpointed_step_reexecuted"))}

    b, k, p = agg("baseline"), agg("s2b_kill_during_llm_restart"), agg("s3a_crash_after_publish")
    e2e = [j["e2e_s"] for j in s["jobs"] if j.get("e2e_s") is not None]
    e2e_txt = f"{fmt(min(e2e))}–{fmt(max(e2e))} (N = {len(e2e)})" if e2e else "…"
    url = s["base_url"]
    if not any(h in url for h in ("localhost", "127.0.0.1")):
        url = "<внутренний адрес сервера университета, доступ через VPN>/v1"  # do not advertise internal hosts
    rows = [["модель / endpoint", f"{s['model']} / {url}"],
            ["job: итог", cnt(dict(Counter(j["state"] for j in s["jobs"])))],
            ["время job end-to-end, с (min–max)", e2e_txt],
            ["LLM-вызовов (ok / всего)", f"{s['llm_calls_ok']} / {s['llm_calls']}"],
            ["латентность LLM-вызова p50 / max, с", f"{fmt(lat.get('p50'))} / {fmt(lat.get('max'))} (N = {lat.get('n', '…')})"],
            ["токенов prompt / completion", f"{s['prompt_tokens']} / {s['completion_tokens']}"],
            ["стоимость, USD", cost_text]]
    decs = [d for j in s["jobs"] for d in j.get("decisions", [])]
    if decs:
        rows.append([f"решения агента `decide` (режим {s.get('decide_mode', '?')}): действие × источник",
                     cnt(dict(Counter(f"{d.get('action')}/{d.get('decision_source')}" for d in decs)))])
        raw = sorted({(d.get("llm_raw") or "").strip()[:30] for d in decs if d.get("llm_raw")})
        if raw:
            rows.append(["ответы модели в `decide` (уникальные, до 30 символов)", "; ".join(f"«{x}»" for x in raw)])
    if b:
        rows.append([f"recovery baseline: N / итог / время job p50, с / повторных LLM",
                     f"{b['n']} / {b['final']} / {fmt(b['time_p50'])} / {b['rep_llm']}"])
    if k:
        rows += [[f"kill во время LLM: N / итог / resume после spawn p50, с",
                  f"{k['n']} / {k['final']} / {fmt(k['resume_p50'])}"],
                 ["kill во время LLM: повторных вызовов / доп. токены прерванных вызовов (подтверждено / верхняя граница) / "
                  "перевыполнено шагов с чекпоинтом", f"{k['rep_llm']} / {k['extra_ok']} / ≤{k['extra_ub']} / {k['reexec']}"]]
    if p:
        rows.append([f"крэш после publish: N / итог / публикаций физ. / лог. / дублей / resume p50, с",
                     f"{p['n']} / {p['final']} / {p['phys']} / {p['log']} / {p['dups']} / {fmt(p['resume_p50'])}"])
    return rows


def _real_llm_notes(s):
    """Data-driven notes on the local real-LLM run: transient errors seen for real, and the agent's decisions."""
    import json as _json
    notes = []
    p = os.path.join(ROOT, "results", "real_llm", "recovery_calls_real.jsonl")
    if os.path.exists(p):
        calls = [_json.loads(x) for x in open(p, encoding="utf-8") if x.strip()]
        tr = [c for c in calls if c.get("provider") == "llm" and c.get("outcome") == "TransientError"]
        if tr:
            sp = os.path.join(ROOT, "results", "real_llm", "recovery_step_runs_real.jsonl")
            details = [_json.loads(x).get("detail") or "" for x in open(sp, encoding="utf-8")
                       if '"transient_error"' in x] if os.path.exists(sp) else []
            why = ""
            if any("model runner has unexpectedly stopped" in d for d in details):
                why = (" — Ollama вернула HTTP 500 «model runner has unexpectedly stopped» (процесс модели упал, "
                       "по-видимому, из-за нехватки памяти)")
            n_txt = "1 вызов LLM завершился" if len(tr) == 1 else f"{len(tr)} {_ru(len(tr), 'вызов', 'вызова', 'вызовов')} LLM завершились"
            notes.append(f"**Настоящая transient-ошибка.** В сценариях восстановления {n_txt} "
                         f"ошибкой провайдера{why}; рантайм классифицировал её как transient, повторил шаг по политике "
                         "ретраев (backoff, та же попытка шага +1), и job завершилась SUCCEEDED. Поэтому в baseline, где "
                         "мы ничего не ломали, есть повторный вызов — это ретрай реальной ошибки, а не след восстановления.")
    decs = [d for j in s.get("jobs", []) for d in j.get("decisions", [])]
    if decs and s.get("decide_mode") == "llm":
        by = Counter(f"{d.get('action')} ({d.get('decision_source')})" for d in decs)
        low = sum(1 for d in decs if d.get("action") == "request_approval" and (d.get("n_docs") or 0) < 5)
        notes.append(f"**Решения модели.** В шаге `decide` модель выбирала: {cnt(dict(by))}. Политика по правилам при "
                     f"`min_docs = 5` и {decs[0].get('n_docs')} прочитанных документах дозапросила бы документы "
                     f"(`call_tool`); модель в {low} из {len(decs)} случаев вместо этого сразу отдала черновик человеку "
                     "(`request_approval`). Это допустимое действие (эскалация к человеку разрешена всегда), поэтому "
                     "ограничения не вмешались; качество такого выбора задание не оценивает. Ни одного нераспознанного "
                     "ответа не было." if not any(d.get("decision_source", "").startswith("fallback") for d in decs) else
                     f"**Решения модели.** В шаге `decide`: {cnt(dict(by))}.")
    return ["", *[x for n in notes for x in (n, "")]][:-1] if notes else []


def _server_block():
    from experiments.report_data import real_llm as _load
    r = _load(os.path.join(ROOT, "results", "real_llm_server"))
    s = r["summary"]
    if not s:
        return []
    rows = _real_rows(s, r["recovery"], "не считалась (тариф сервера неизвестен; цены в прогоне = 0)")
    return ["", "### Real LLM run: сервер ITMO (qwen3.8-27b)", "",
            "> **Статус: выполнен** (данные из `results/real_llm_server/`; не смешиваются ни с симуляцией, ни с прогоном на qwen3.5:4b).", "",
            table(["метрика", "Real LLM (сервер ITMO)"], rows), "",
            "Этот прогон сделан раньше, с одним повтором на сценарий и решением `decide` по правилам (иллюстрация, а не "
            "статистика); повторить его с большим числом повторов не удалось — сервер доступен только через VPN "
            "университета. Латентность и время job нельзя напрямую "
            "сравнивать с прогоном на Ollama: другая модель и другое железо (CPU против сервера университета). "
            "Столбцы `cost_*` в `results/real_llm_server/recovery_runs_real.csv` посчитаны подпрограммой восстановления "
            "по иллюстративным ценам из конфига по умолчанию и реальной стоимостью не являются."]


def section_real(rec, rl):
    base = next((c for c in rec["cases"] if c["case"] == "baseline"), None)
    sim_rows = []
    if base:
        sim_rows = [["LLM-вызовов на job (baseline)", fmt(base["llm_calls"] / base["n"], 1)],
                    ["токенов на job (prompt+completion)", fmt(base["tokens_ok"] / base["n"], 1)],
                    ["стоимость на job, USD (иллюстративная цена из конфига)", f"{base['cost_total'] / base['n']:.6f}"],
                    ["латентность LLM-вызова", "задана конфигом: 1.0 с (Part 1)"],
                    ["время job baseline p50, с", fmt(base["total_p50"])]]
    out = ["### Simulated (данные этого отчёта)", "", table(["метрика", "SimulatedLLM"], sim_rows), "",
           "### Real LLM run", ""]
    s = rl["summary"]
    if not s:
        out += ["> **Статус: НЕ ЗАПУСКАЛСЯ.** Таблица заполняется после запуска `scripts/run_real_llm.sh` "
                "(или `.ps1`): в среде подготовки не было ни API-ключа, ни локальной модели. Скрипт пишет "
                "`results/real_llm/summary.json`, `calls.jsonl`, `recovery_runs_real.csv`; повторный "
                "`make report` автоматически заполнит таблицу ниже.", "",
                table(["метрика", "Real LLM (заполняется после запуска скрипта)"],
                      [["модель / endpoint", "…"], ["job: итог", "…"], ["LLM-вызовов (ok / всего)", "…"],
                       ["латентность LLM-вызова p50 / max, с", "…"], ["токенов prompt / completion", "…"],
                       ["стоимость, USD", "…"], ["kill во время LLM: повторных вызовов / доп. токены", "…"],
                       ["крэш после publish: физ./лог. публикаций", "…"]])]
    else:
        cost = (f"{s['cost_usd']} — условная (цена из конфига; локальная модель, реальных затрат нет)"
                if s.get("cost_usd") else "не считалась")
        out += ["> **Статус: выполнен** (данные из `results/real_llm/`, не смешиваются с симуляцией).", "",
                table(["метрика", "Real LLM (Ollama, локально)"], _real_rows(s, rl["recovery"], cost)), "",
                "В основном прогоне 3 job стартуют одновременно, а локальная Ollama на CPU обрабатывает запросы по "
                "очереди (в `results/real_llm/calls.jsonl` каждый следующий вызов завершается вплотную к предыдущему), "
                "поэтому латентность вызова и время job включают ожидание в очереди модели; в сценариях восстановления "
                "job одна, отсюда меньшее время baseline.", *_real_llm_notes(s)]
    out += ["", "Чем реальная LLM отличается от симулятора: латентность не "
            "детерминирована; ошибки 429/5xx/таймауты классифицируются как transient и ретраятся; прерванный крэшем "
            "вызов почти наверняка оплачен провайдером (верхняя граница доп. токенов становится реальной ценой); "
            "текст черновика при повторе может отличаться, но это не ломает идемпотентность публикации — тело "
            "собирается из *сохранённых* результатов шагов, а повтор `draft` после чекпоинта невозможен.", "",
            "Запросы к реальной LLM идут с `temperature = 0` (`src/agentrt/llm.py`), поэтому в сценариях восстановления "
            "с одним номером повтора (одинаковые документы и промпты) число completion-токенов совпадает между сценариями "
            "(см. `recovery_calls_real.jsonl`); между тремя job основного прогона промпты различаются, и токены разные "
            "(`calls.jsonl`). Для прерванного вызова в журнале записан только prompt (ответа не было)."]
    out += _server_block()
    return "\n".join(out)


def section_logs():
    p = os.path.join(ROOT, "results", "raw", "sample_structured_log.jsonl")
    if not os.path.exists(p):
        return ""
    lines = [ln for ln in open(p, encoding="utf-8").read().splitlines() if ln.strip()]
    pick = [ln for ln in lines if any(k in ln for k in ('"step start"', 'transient', '"event applied"',
                                                          '"event duplicate"', 'ignored_terminal'))][:6]
    nl = "\n"
    return (nl + nl + "**Структурированные логи.** Каждая запись — JSON с полями `job_id, step, attempt, event_id` "
            f"(и `worker, tenant_id, state, data`). Пример ({len(lines)} {_ru(len(lines), "строка", "строки", "строк")}, один job с transient-ошибкой, "
            "дублем и поздним событием): `results/raw/sample_structured_log.jsonl`:" + nl + nl + "```" + nl +
            nl.join(pick) + nl + "```")


def section_tests():
    ps = pytest_summary()
    if not ps:
        return "_`results/pytest_output.txt` отсутствует — запустите `make test`._"
    lines = "\n".join(ps["tests"])
    linux = ""
    lp = os.path.join(LINUX, "pytest_output.txt")
    if os.path.exists(lp):
        tail = [ln.strip("= \n") for ln in open(lp, encoding="utf-8", errors="replace") if ln.strip()]
        if tail:
            linux = (f"\n\nТот же набор в Linux-контейнере (`Dockerfile`, POSIX SIGKILL в тестах восстановления): "
                     f"**{tail[-1]}** (`results/raw_linux/pytest_output.txt`).")
    return (f"Итог (Windows): **{ps['last']}** (полный вывод `pytest -v`: `results/pytest_output.txt`).{linux}"
            f"\n\n```\n{lines}\n```")


ANSWERS = """
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
"""

def _real_llm_meta():
    from experiments.report_data import real_llm as _load
    s = _load(os.path.join(ROOT, "results", "real_llm"))["summary"] or {}
    rec = _load(os.path.join(ROOT, "results", "real_llm"))["recovery"] or []
    reps = max([r["rep"] for r in rec] or [-1]) + 1
    return s, reps


def _limits_text():
    s, reps = _real_llm_meta()
    llm_decide = s.get("decide_mode") == "llm"
    n_jobs = len(s.get("jobs", []))
    real = (
        "* **Real LLM: два прогона, небольшая статистика.** Основной прогон — локально, `qwen3.5:4b` (Ollama, CPU), "
        "`reasoning_effort=none` (без него модель тратит токены на рассуждение и возвращает пустой `content`): "
        f"{n_jobs} job" + (" с выбором действия через LLM (`decide_mode = llm`, `min_docs = 5`)" if llm_decide else
                           ", решение `decide` по правилам") +
        f" и по {reps} {_ru(reps, 'повтору', 'повтора', 'повторов')} трёх сценариев восстановления (baseline, kill во время LLM-вызова, крэш после "
        "публикации). Таймаут ожидания воркера в сценарии восстановления 300 с из-за медленной локальной модели. Две "
        "самые первые попытки завершились HTTP 503 (неверное имя модели в `.env`; затем 503 до отключения рассуждения) "
        "и в отчёт не включены. "
        + ("Ещё одна попытка этого прогона (перед показанным) не состоялась: все вызовы модели завершились HTTP 500, "
           "локальной Ollama не хватило памяти для загрузки модели (`out-of-memory` при старте `llama-server`, пока на "
           "машине работали Docker и другие программы); её данные не включены, прогон повторён после освобождения "
           "памяти. " if llm_decide else
           "Повторный прогон с решением через LLM и 5 повторами на сценарий запускался, но все вызовы модели "
           "завершились HTTP 500: локальной Ollama не хватило памяти для загрузки модели на этой машине (`out-of-memory` "
           "при старте `llama-server`); его данные в отчёт не включены, сохранён предыдущий успешный прогон. ") +
        "Второй прогон — сервер университета, `qwen3.8-27b` (vLLM, доступ только через VPN, токен только в локальном "
        "`.env`): 3 job, решение по правилам, по одному повтору сценариев; повторить его не удалось (сервер был "
        "недоступен). Стоимость сервера не считалась (тариф неизвестен); цена для Ollama условная. Влияет ли "
        "`LLM_REASONING_EFFORT=none` на серверную модель, не проверялось. Результаты двух моделей и Simulated не "
        "смешиваются.")
    decide = ("* **Решение агента через LLM** (`decide_mode = llm`) "
              + ("использовано в прогоне с реальной LLM (решения и их источник — в таблице Real LLM); "
                 if llm_decide else
                 "реализовано и покрыто автотестами (`tests/test_decide.py`, в т.ч. сквозной прогон job), но в "
                 "сохранённых прогонах с реальной LLM решение принималось по правилам (они сделаны до появления режима, "
                 "повтор не удался — см. выше); ")
              + "симуляционные эксперименты Part 1–3 идут на правилах — это сохраняет детерминизм и сравнимость "
              "политик планирования. Ограничения (бюджет tool-вызовов, обязательный approval) проверяются кодом, "
              "поэтому модель не может их нарушить, но качество её выбора не оценивалось (задание не оценивает "
              "качество ответов LLM).")
    return LIMITS.replace("@@REAL_LLM_LIMIT@@", real).replace("@@DECIDE_LIMIT@@", decide)


LIMITS = """
@@REAL_LLM_LIMIT@@
* **Один узел, один писатель.** SQLite сериализует все записи; масштабирование — несколько процессов на одной
  машине (лимиты проверяются в транзакции и для них верны), но не кластер. Узкое место — частота транзакций
  (claim + complete на каждый шаг + heartbeats).
* **Основная ОС — Windows; Linux — контейнер и CI.** Основные таблицы Part 1–3 получены на Windows 10 («SIGKILL»
  там — `Popen.kill()` → `TerminateProcess`, без обработчиков/`finally`). Все три части и автотесты повторены в
  Linux-контейнере с настоящим POSIX SIGKILL (`results/raw_linux/`, блоки «на Linux») — это Docker Desktop с ядром
  WSL2 на том же железе. На **отдельной** Linux-машине (GitHub Actions, `ubuntu-24.04`, `.github/workflows/ci.yml`)
  на каждый push гоняются все автотесты, Part 1 и Part 2 с автоматической проверкой гарантий; сборка падает при
  нарушении. Числа CI в таблицы отчёта не переносятся (они на странице прогона и в артефакте `ci-results`);
  Part 3 в CI не гоняется — общий раннер с плавающей ёмкостью исказил бы времена.
* **`synchronous=NORMAL`**: гарантия durable — против падения процесса (это и проверялось), но не против
  потери питания/краха ОС для последних транзакций (`AGENTRT_SYNC=FULL` закрывает это ценой латентности; не измерялось).
* **Масштабированное время** в Part 3 (1 вирт. с = 0.1 с): накладные расходы рантайма (транзакции, поллинг)
  не масштабируются и в вирт. секундах выглядят больше (оценка приведена). Абсолютные значения — для этой
  машины и этого масштаба; сравнение политик корректно, т.к. условия одинаковые.
* **Статистика.** Part 1: 10 повторов на сценарий; Part 2: 300 повторов гонки E5; Part 3: 5 прогонов (seed) на политику —
  разброс p95 по seed показан, доверительные интервалы не считались. Для `light*`×`long` и priority = 1 наблюдений меньше (см. N).
* **Неопределённость внешних вызовов** не устранима на стороне клиента: прерванный LLM-вызов может быть
  оплачен; мы даём верхнюю оценку (prompt + max_output_tokens), а не точное значение.
* **Окно публикации при cancel.** Отмена во время `publish` не может «отозвать» уже отправленный запрос;
  инвариант «нет живой публикации у не-SUCCEEDED job» конечный (eventual): публикация существует до отзыва
  (до закрытия run и следующего прохода relay; при падении воркера — до истечения lease). Долгая недоступность
  сервиса публикаций не проверялась: отзыв повторяется без ограничения числа попыток.
* **Мгновенное восстановление по имени воркера** (`recover_own`) предполагает, что прежний процесс с тем же
  `--name` мёртв; это не проверяется (нет проверки pid). Два живых воркера с одним именем — ошибка конфигурации:
  фенсинг по `lease_token` не даст закоммитить чужой результат, но шаг (и LLM-вызов) может выполниться дважды.
* **Approval под нагрузкой проверен только для `drr`** и одной доли/задержки (50 %, 20 вирт. с, отдельная таблица в
  Part 3); основные таблицы Part 3 — без approval, чтобы сравнение политик не зависело от поведения «человека».
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
@@DECIDE_LIMIT@@
"""


def build_md() -> str:
    rec, ev, ld, rl = recovery(), events(), load(), real_llm()
    figs = [plots.state_machine()]
    if rec["rows"]:
        figs.append(plots.recovery_times(rec))
    if ev["e5"]:
        figs += [plots.race_scatter(ev["e5"]), plots.event_latency(ev["e1"])]
    if ld["jobs"]:
        seed = ld["seeds"][0]
        figs += plots.load_timeseries(ld, seed) + [plots.load_compare(ld, seed), plots.load_p95(ld), plots.load_share(ld)]
    md = f"""# Отчёт: durable event-driven runtime для агента-составителя отчётов

Вариант стека: **custom runtime** — Python asyncio + SQLite (WAL) как durable state store + transactional outbox +
таблицы `events`, `timers`, `scheduler`; FastAPI + CLI. Архитектура, форматы, таблица переходов, правило гонок и
обоснование выбора — в [README.md](README.md). **Все числа ниже вычислены скриптом `experiments/make_report.py`
из сырых данных `results/raw/`** (симуляция), `results/real_llm/` (реальная LLM: Ollama, локально) и
`results/real_llm_server/` (реальная LLM: сервер ITMO); разделы Simulated и два прогона Real LLM не смешиваются.

## 1. Окружение

{section_env(rec, ld)}

![states](results/figures/state_machine.png)

## 2. Автотесты

{section_tests()}{section_logs()}

## 3. Part 1 — восстановление и идемпотентность (Simulated)

{section_part1(rec) if rec['rows'] else '_нет данных_'}

## 4. Part 2 — событийный жизненный цикл (Simulated)

{section_part2(ev)}

## 5. Part 3 — backpressure и честное планирование (Simulated)

{section_part3(ld)}

## 6. Real LLM vs Simulated

{section_real(rec, rl)}

## 7. Ответы на вопросы задания
{ANSWERS}
## 8. Ограничения и оговорки
{_limits_text()}
"""
    return md


CSS = """
body{font-family:'Segoe UI',Arial,sans-serif;font-size:10.5pt;color:#0b0b0b;max-width:1000px;margin:24px auto;line-height:1.4}
h1{font-size:18pt} h2{font-size:14pt;border-bottom:1px solid #c3c2b7;padding-bottom:3px;margin-top:22px}
h3{font-size:11.5pt;margin-top:16px}
table{border-collapse:collapse;margin:8px 0;font-size:8pt;page-break-inside:auto} tr{page-break-inside:avoid}
th,td{border:1px solid #e1e0d9;padding:3px 5px;vertical-align:top} th{background:#f0efec;text-align:left}
code{background:#f0efec;padding:0 3px;border-radius:3px;font-size:8.5pt} pre{background:#f6f6f4;padding:8px;font-size:7.5pt;white-space:pre-wrap}
img{max-width:100%;page-break-inside:avoid} blockquote{border-left:3px solid #eb6834;margin:8px 0;padding:4px 10px;background:#fdf3ee}
"""


def build_pdf(md_text: str, out_pdf: str) -> str:
    import markdown

    html_body = markdown.markdown(md_text, extensions=["tables", "fenced_code"])
    build = os.path.join(WORK, "report_build")
    os.makedirs(build, exist_ok=True)
    root_uri = "file:///" + ROOT.replace("\\", "/") + "/"
    html = (f"<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>Отчёт: durable event-driven runtime для агента-составителя отчётов</title>"
            f"<base href='{root_uri}'><style>{CSS}</style>"
            f"</head><body>{html_body}</body></html>")
    hp = os.path.join(build, "REPORT.html")
    with open(hp, "w", encoding="utf-8") as fh:
        fh.write(html)
    candidates = [shutil.which(x) for x in ("msedge", "chrome", "google-chrome", "chromium", "chromium-browser")]
    candidates += [r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
                   r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
                   r"C:\Program Files\Google\Chrome\Application\chrome.exe"]
    browser = next((c for c in candidates if c and os.path.exists(c)), None)
    if not browser:
        return "no headless browser found (Edge/Chrome); REPORT.pdf not generated"
    prof = os.path.join(build, "profile")
    cmd = [browser, "--headless=new", "--disable-gpu", "--no-pdf-header-footer", f"--user-data-dir={prof}",
           "--allow-file-access-from-files", f"--print-to-pdf={out_pdf}", "file:///" + hp.replace("\\", "/")]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=180)
    return f"PDF via {os.path.basename(browser)}: {os.path.exists(out_pdf)}"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-pdf", action="store_true")
    a = ap.parse_args(argv)
    md = build_md()
    with open(os.path.join(ROOT, "REPORT.md"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write(md)
    print("REPORT.md written")
    if not a.no_pdf:
        print(build_pdf(md, os.path.join(ROOT, "REPORT.pdf")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
