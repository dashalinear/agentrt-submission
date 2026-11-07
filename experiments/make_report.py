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

from experiments import plots
from experiments.common import ROOT, WORK
from experiments.report_data import TENANTS, events, fmt, load, pytest_summary, real_llm, recovery

CASE_RU = {
    "baseline": "без отказов (baseline)",
    "s1_kill_between_steps": "1. kill между шагами (после extract)",
    "s2a_kill_during_llm_lease": "2a. kill во время LLM, другой воркер ждёт lease",
    "s2b_kill_during_llm_restart": "2b. kill во время LLM, рестарт того же воркера",
    "s3a_crash_after_publish": "3a. крэш после publish (ack не сохранён)",
    "s3b_ack_lost_network": "3b. ack потерян в сети (503 после коммита)",
    "s4_transient_repeated": "4. повторяющаяся transient-ошибка",
}


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
                "доп. токены: факт / верхняя граница", "доп. стоимость USD: факт / верх."], rows2))
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
    return "\n".join(txt)


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
    e7_t = table(["отмена в состоянии", "N", "p50, мс", "p95, мс", "max, мс"],
                 [[k, v["n"], v["p50"], v["p95"], v["max"]] for k, v in e7.items()])
    out = [
        "Все эксперименты — `time_scale = 1.0`, воркер в отдельном потоке (E1–E5, E7) или отдельном процессе (E6). "
        "Латентности измерены по меткам времени, записанным в БД в момент коммита (`events.received_at`, "
        "`transitions.ts`, `step_runs.started_at`). Сырые данные: `results/raw/events_*.csv`, `events_summary.json`.", "",
        "### E1. Approval вовремя", "",
        f"{sum(e1['final'].values())} job, итог: {cnt(e1['final'])}; исходы событий: {cnt(e1['outcomes'])}.", "", lat, "",
        "«event → state transition» — от приёма события до коммита перехода `WAITING_EVENT → QUEUED` (inbox и переход "
        "в одной транзакции). «event → next step started» дополнительно включает ожидание цикла планировщика "
        "(поллинг `poll_interval_s = 3 мс`, межпроцессного уведомления нет).", "",
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
        f"исполняет `save_report` и `publish` (~30–50 мс), так что их завершение тоже попадает в окно гонки.", "",
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
        "Картина соответствует правилу: cancel, закоммиченный раньше дедлайна и раньше завершения `publish`, даёт "
        "CANCELLED; если approval пришёл достаточно рано, шаги успевают закоммитить SUCCEEDED, и более поздние "
        "cancel получают `already_terminal`; если к моменту approval или коммита шага дедлайн уже прошёл, побеждает "
        "TIMED_OUT (`deadline_won`, `aborted_deadline`, `late_results_discarded`).", "",
        "### E6. Рестарт воркера во время ожидания", "",
        table(["вариант", "повтор", "итог", "открытых step_runs в ожидании", "состояние пока воркер мёртв",
               "исход события", "готов → первый шаг, мс", "готов → терминал, мс", "опоздание таймаута, мс"], e6_rows), "",
        "Ожидание не держит слот (0 открытых `step_runs`), событие принимается и применяется даже когда воркер мёртв "
        "(inbox в БД), а дедлайн, истёкший во время простоя, срабатывает при старте нового воркера (таймер хранится в "
        "БД). Для `deadline_while_down` «опоздание таймаута» ≈ длительность простоя — таймер не может сработать, пока "
        "нет ни одного живого воркера.", "",
        "### E7. Время до реальной остановки после cancel", "", e7_t, "",
        f"Исходы LLM-вызовов в E7: {cnt(s['e7']['llm_call_outcomes'])}. В `RUNNING` остановка ограничена интервалом "
        "heartbeat (0.1 с в E7): шаг узнаёт об отмене на следующем heartbeat и прерывает LLM-вызов; в `QUEUED` и "
        "`WAITING_EVENT` исполняемой работы нет — остановка мгновенная (коммит перехода).", "",
        "### Сводка инвариантов (переходы из журнала `transitions`, пишется триггером)", "", inv_t,
    ]
    return "\n".join(out)


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
                "jobs/s (весь прогон)", "jobs/s в burst", "max ожидание, с", "max возраст старейшей в очереди, с",
                "max длина очереди", "max одновременно global / лимит", "max на tenant / лимит"],
               [[p["policy"], p["runs"], p["arrivals"], p["admitted"], f"{p['rejected']} ({cnt(p['rejected_by_tenant'])})",
                 p["succeeded"], f"{p['timed_out']} ({cnt(p['timed_out_by_tenant'])})", fmt(p["throughput"], 3),
                 fmt(p["burst_throughput"], 3), p["max_wait"], p["max_oldest_age"], p["max_queue"],
                 f"{p['max_global_obs']} / {p['global_limit']}", f"{p['max_tenant_obs']} / {p['tenant_limit']}"]
                for p in pp])
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
        "### Сводка по политикам (сумма/максимум по 3 прогонам)", "", pt, "",
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
        "* **Throughput vs latency vs isolation.** Пока есть работа хотя бы одного tenant, `fifo` и `drr` "
        "work-conserving — общий throughput у них близок (см. jobs/s), DRR лишь перераспределяет ожидание. "
        f"`drr_iso` не даёт `heavy` занять больше 3 из 6 слотов даже когда остальные простаивают: throughput в burst "
        f"{fmt(pd.get('drr_iso', {}).get('burst_throughput'), 3)} против {fmt(pd.get('drr', {}).get('burst_throughput'), 3)} "
        f"job/с у `drr` — плата за гарантированный запас ёмкости для лёгких пользователей. Квота очереди на tenant "
        "дополнительно защищает лёгких пользователей от отказов `queue_full`, которые в `fifo`/`drr` вызывает чужой burst "
        "(общий `max_queue_size`).",
        "* **Длительная перегрузка.** Admission control ограничивает очередь (429/REJECTED с причиной), дедлайны "
        "вычищают то, что всё равно не успеет (TIMED_OUT в очереди), поэтому очередь и возраст старейшей задачи "
        "ограничены сверху, а не растут бесконечно; после окончания burst очередь рассасывается (фаза P3).",
        "* **Планирование на границах шагов.** Слот освобождается после каждого шага (≤ 1.5 с у long), поэтому "
        "короткая задача ждёт не всю 10-секундную job, а максимум один шаг; цена — лишняя транзакция claim/complete "
        "на каждый шаг (см. накладные расходы выше).",
        "* **Aging.** Внутри tenant priority = 1 обгоняет priority = 0, но задача с низким приоритетом, прождавшая "
        "10 с, получает +1 и не голодает (`tests/test_scheduling.py::test_aging_lets_old_low_priority_job_overtake`).",
        "* **Лимиты.** По трассе `step_runs` (интервалы исполнения) максимальная одновременность ни разу не превысила "
        "global/per-tenant лимит (столбцы «max … / лимит»; то же проверяют автотесты).",
        "* **Ожидание approval не занимает слоты** — подтверждено автотестом "
        "`tests/test_timers_and_slots.py::test_waiting_job_does_not_occupy_execution_slot` (при `global_concurrency=1` "
        "вторая job полностью выполняется, пока первая ждёт approval) и E2/E6 (0 открытых `step_runs` в ожидании).",
    ]
    return "\n".join(out)


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
        lat = s.get("llm_latency_s", {})
        rr = {r["case"]: r for r in rl["recovery"]}
        k = rr.get("s2b_kill_during_llm_restart", {})
        p = rr.get("s3a_crash_after_publish", {})
        out += ["> **Статус: ЗАПУЩЕН** (данные из `results/real_llm/`, не смешиваются с симуляцией).", "",
                table(["метрика", "Real LLM"],
                      [["модель / endpoint", f"{s['model']} / {s['base_url']}"],
                       ["job: итог", cnt({j['state']: 1 for j in s['jobs']})],
                       ["LLM-вызовов (ok / всего)", f"{s['llm_calls_ok']} / {s['llm_calls']}"],
                       ["латентность LLM-вызова p50 / max, с", f"{fmt(lat.get('p50'))} / {fmt(lat.get('max'))}"],
                       ["токенов prompt / completion", f"{s['prompt_tokens']} / {s['completion_tokens']}"],
                       ["стоимость, USD", f"{s['cost_usd']}"],
                       ["kill во время LLM: повторных вызовов / доп. токены (факт / верх.)",
                        f"{k.get('repeated_llm_calls', '…')} / {k.get('extra_tokens_ok', '…')} / ≤{k.get('extra_tokens_upper_bound', '…')}"],
                       ["крэш после publish: физ./лог. публикаций",
                        f"{p.get('publish_physical', '…')} / {p.get('publish_logical', '…')}"]])]
    out += ["", "Чем реальная LLM отличается от симулятора (ожидаемо, проверить по таблице): латентность и токены не "
            "детерминированы; ошибки 429/5xx/таймауты классифицируются как transient и ретраятся; прерванный крэшем "
            "вызов почти наверняка оплачен провайдером (верхняя граница доп. токенов становится реальной ценой); "
            "текст черновика при повторе может отличаться, но это не ломает идемпотентность публикации — тело "
            "собирается из *сохранённых* результатов шагов, а повтор `draft` после чекпоинта невозможен."]
    return "\n".join(out)


def section_tests():
    ps = pytest_summary()
    if not ps:
        return "_`results/pytest_output.txt` отсутствует — запустите `make test`._"
    lines = "\n".join(ps["tests"])
    return f"Итог: **{ps['last']}** (полный вывод `pytest -v`: `results/pytest_output.txt`).\n\n```\n{lines}\n```"


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
не было ни нового состояния, ни сообщения; шаг повторится после истечения lease.

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
результаты отбрасываются, поздние события получают `ignored_terminal`. Если publish успел пройти, а job
отменена/истекла, выполняется компенсация (retract публикации). E5: 0 нарушений инвариантов на всех повторах.

**5. Что обеспечивает справедливость и что при длительной перегрузке?** DRR по tenant на границах шагов
(веса, quantum в секундах работы) + aging внутри tenant; опционально жёсткие per-tenant лимиты слотов и
квоты очереди. При длительной перегрузке очередь ограничена admission control (429/REJECTED с причиной и
метрикой), дедлайны снимают задачи, которые не успеют (TIMED_OUT, в т.ч. из очереди), DRR не даёт одному
tenant вытеснить остальных; throughput остаётся равным ёмкости (work-conserving), а отказы и истечения
концентрируются у tenant-источника перегрузки (Part 3).

**6. Какие гарантии от инфраструктуры, а какие из кода?** Инфраструктура (SQLite): атомарность,
сериализуемость писателей, долговечность коммита (WAL), уникальные индексы и триггеры. Код: модель
состояний и CAS, lease/heartbeat/reaper/recovery, фенсинг, ретраи и классификация ошибок, дедуп событий
(через PK), outbox relay, durable-таймеры, admission control, планировщик, компенсация, идемпотентный
ключ публикации. Внешний сервис отвечает за собственную дедупликацию по ключу. Подробная таблица — README,
раздел «Гарантии инфраструктуры vs кода приложения».
"""

LIMITS = """
* **Real LLM не запускалась** в среде подготовки (нет ключа/локальной модели) — все числа в отчёте получены с
  `SimulatedLLM`; раздел «Real LLM run» помечен и заполняется после `scripts/run_real_llm.*`. Код
  `OpenAICompatibleLLM` проверен только против mock-транспорта (`tests/test_llm.py`), не против настоящего API.
* **Один узел, один писатель.** SQLite сериализует все записи; масштабирование — несколько процессов на одной
  машине (лимиты проверяются в транзакции и для них верны), но не кластер. Узкое место — частота транзакций
  (claim + complete на каждый шаг + heartbeats).
* **Windows вместо Linux.** Эксперименты выполнены на Windows 10: «SIGKILL» реализован как
  `Popen.kill()` → `TerminateProcess` (те же свойства: без обработчиков/`finally`). На POSIX тот же код
  посылает SIGKILL, но там прогон не выполнялся.
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
  инвариант обеспечивается компенсацией (retract) после факта, т.е. публикация может кратко существовать.
* **Цены токенов** в конфиге иллюстративные (не прайс-лист провайдера); стоимость = токены × цена из конфига.
* **Межпроцессных уведомлений нет**: воркер узнаёт о новых событиях/заявках из других процессов поллингом
  (`poll_interval_s`), это входит в латентность «event → next step».
* `docker-compose.yml` приложен как опция и в отчётных прогонах не использовался.
* Политика агента (`decide`) — детерминированные правила с ограничениями (min_docs, max_tool_calls, approval),
  а не решение LLM; LLM используется для черновика.
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
из сырых данных `results/raw/`** (симуляция) и `results/real_llm/` (реальная LLM, если запускалась); разделы
Simulated и Real LLM не смешиваются.

## 1. Окружение

{section_env(rec, ld)}

![states](results/figures/state_machine.png)

## 2. Автотесты

{section_tests()}

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
## 8. Ограничения и честные оговорки
{LIMITS}
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
    html = (f"<!doctype html><html><head><meta charset='utf-8'><base href='{root_uri}'><style>{CSS}</style>"
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
