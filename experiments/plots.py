"""Figures for REPORT.md (matplotlib, Agg). Every figure is drawn from results/raw only."""
from __future__ import annotations

import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch  # noqa: E402

from experiments.common import FIG  # noqa: E402

# validated categorical palette (fixed order) + neutral chart chrome
S1, S2, S3, S4 = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, INK2, GRID, AXIS, SURF = "#0b0b0b", "#52514e", "#e1e0d9", "#c3c2b7", "#fcfcfb"
TENANT_COLORS = {"heavy": S1, "light1": S2, "light2": S3}
POLICY_COLORS = {"fifo": S1, "drr": S2, "drr_iso": S3}
OUTCOME_COLORS = {"SUCCEEDED": S1, "CANCELLED": S2, "TIMED_OUT": S3, "FAILED": S4}

plt.rcParams.update({
    "figure.facecolor": SURF, "axes.facecolor": SURF, "axes.edgecolor": AXIS, "axes.labelcolor": INK2,
    "xtick.color": INK2, "ytick.color": INK2, "text.color": INK, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.6, "axes.spines.top": False, "axes.spines.right": False, "font.size": 9,
    "axes.titlesize": 10, "axes.titleweight": "bold", "legend.frameon": False, "savefig.dpi": 150,
})


def _save(fig, name):
    os.makedirs(FIG, exist_ok=True)
    p = os.path.join(FIG, name)
    fig.savefig(p, bbox_inches="tight")
    plt.close(fig)
    return f"results/figures/{name}"


def state_machine():
    pos = {"QUEUED": (1, 2), "RUNNING": (3, 2), "WAITING_EVENT": (5, 2), "SUCCEEDED": (3, 3.6), "FAILED": (5, 3.6),
           "CANCELLED": (2, 0.4), "TIMED_OUT": (4, 0.4)}
    from agentrt.states import ALLOWED_TRANSITIONS, TERMINAL

    fig, ax = plt.subplots(figsize=(7.5, 4))
    ax.axis("off")
    ax.grid(False)
    for (a, b) in ALLOWED_TRANSITIONS:
        (x1, y1), (x2, y2) = pos[a], pos[b]
        rad = 0.25 if (b, a) in ALLOWED_TRANSITIONS else 0.0
        if (a, b) == ("WAITING_EVENT", "QUEUED"):
            rad = -0.35  # arc over RUNNING instead of through it
        import math

        ang = math.atan2((y2 - y1) * 69, (x2 - x1) * 95)  # approx. points per data unit (y, x)

        def shrink(label):  # distance from box centre to its border along the arrow direction
            hw, hh = 3.1 * len(label) + 6, 13
            c, s_ = abs(math.cos(ang)), abs(math.sin(ang))
            return min(hw / c if c > 1e-6 else 1e9, hh / s_ if s_ > 1e-6 else 1e9) + 3

        ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=11, color=AXIS,
                                     lw=1.2, connectionstyle=f"arc3,rad={rad}", shrinkA=shrink(a),
                                     shrinkB=shrink(b)))
    for s, (x, y) in pos.items():
        term = s in TERMINAL
        ax.text(x, y, s, ha="center", va="center", fontsize=8.5, color="white" if not term else INK,
                bbox=dict(boxstyle="round,pad=0.45", fc=S1 if not term else "#f0efec", ec=S1 if not term else AXIS))
    ax.set_xlim(0.2, 5.9)
    ax.set_ylim(-0.1, 4.1)
    ax.set_title("Job state machine (allowed transitions; terminal states are immutable)")
    return _save(fig, "state_machine.png")


def recovery_times(rec):
    cases = [c["case"] for c in rec["cases"]]
    fig, ax = plt.subplots(figsize=(7.5, 3.2))
    for i, c in enumerate(cases):
        xs = [r["total_time_s"] for r in rec["rows"] if r["case"] == c]
        ax.scatter(xs, [i] * len(xs), s=28, color=S1, zorder=3, edgecolor=SURF, linewidth=1.5)
    if rec["baseline_p50"] == rec["baseline_p50"]:
        ax.axvline(rec["baseline_p50"], color=INK2, lw=1, ls="--")
        ax.text(rec["baseline_p50"], len(cases) - 0.4, " baseline p50", color=INK2, fontsize=8)
    ax.set_yticks(range(len(cases)), cases)
    ax.invert_yaxis()
    ax.set_xlabel("job time, created → terminal (s, real)")
    ax.set_title("Part 1: job duration with and without failures (each dot = one run)")
    return _save(fig, "recovery_job_time.png")


def race_scatter(e5):
    fig, ax = plt.subplots(figsize=(6.5, 4))
    for outcome, col in OUTCOME_COLORS.items():
        rs = [r for r in e5 if r["final"] == outcome]
        if rs:
            ax.scatter([r["approve_offset_ms"] for r in rs], [r["cancel_offset_ms"] for r in rs], s=14, color=col,
                       label=f"{outcome} (n={len(rs)})", edgecolor=SURF, linewidth=0.8, zorder=3)
    ax.axhline(0, color=AXIS, lw=1)
    ax.axvline(0, color=AXIS, lw=1)
    ax.set_xlabel("approval sent at deadline + Δ (ms)")
    ax.set_ylabel("cancel sent at deadline + Δ (ms)")
    ax.set_title("Part 2 / E5: outcome of approval + cancel + deadline races")
    ax.legend(loc="upper left", fontsize=8)
    return _save(fig, "events_race_outcomes.png")


def event_latency(e1):
    xs = [r["event_to_transition_ms"] for r in e1 if r["event_to_transition_ms"] is not None]
    ys = [r["event_to_next_step_start_ms"] for r in e1 if r["event_to_next_step_start_ms"] is not None]
    fig, axs = plt.subplots(1, 2, figsize=(7.5, 2.8))
    axs[0].hist(xs, bins=30, color=S1, edgecolor=SURF)
    axs[0].set_title(f"event → state transition (n={len(xs)})")
    axs[0].set_xlabel("ms")
    axs[1].hist(ys, bins=30, color=S1, edgecolor=SURF)
    axs[1].set_title(f"event → next step started (n={len(ys)})")
    axs[1].set_xlabel("ms")
    return _save(fig, "events_latency.png")


def load_timeseries(ld, seed):
    paths = []
    for p in ld["policies"]:
        pts = [x for x in ld["ts"] if x["policy"] == p and x["seed"] == seed]
        if not pts:
            continue
        t = [x["t_v"] for x in pts]
        fig, axs = plt.subplots(2, 1, figsize=(7.5, 4.6), sharex=True)
        for ten, col in TENANT_COLORS.items():
            axs[0].plot(t, [x[f"queued_{ten}"] for x in pts], color=col, lw=1.6, label=ten)
            axs[1].plot(t, [x[f"active_{ten}"] for x in pts], color=col, lw=1.4, label=ten)
        for ax in axs:
            for ph in ld["meta"].get("phases", []):
                ax.axvline(ph[1], color=AXIS, lw=0.8, ls=":")
        axs[0].set_ylabel("runnable queued jobs")
        axs[1].set_ylabel("active steps (slots)")
        axs[1].set_xlabel("virtual time (s)")
        axs[0].set_title(f"Part 3: queue length and active steps per tenant — policy={p}, seed={seed}")
        axs[0].legend(ncol=3, loc="upper right", fontsize=8)
        paths.append(_save(fig, f"load_timeseries_{p}.png"))
    return paths


def load_compare(ld, seed):
    fig, axs = plt.subplots(2, 1, figsize=(7.5, 4.6), sharex=True)
    for p in ld["policies"]:
        pts = [x for x in ld["ts"] if x["policy"] == p and x["seed"] == seed]
        t = [x["t_v"] for x in pts]
        axs[0].plot(t, [x["queued_total"] for x in pts], color=POLICY_COLORS.get(p, S4), lw=1.6, label=p)
        axs[1].plot(t, [x["oldest_queued_age_v"] for x in pts], color=POLICY_COLORS.get(p, S4), lw=1.6, label=p)
    axs[0].set_ylabel("queued (all tenants)")
    axs[1].set_ylabel("age of oldest queued item (s)")
    axs[1].set_xlabel("virtual time (s)")
    axs[0].set_title(f"Part 3: total queue and oldest queued age by policy (seed={seed})")
    axs[0].legend(ncol=3, fontsize=8)
    return _save(fig, "load_queue_by_policy.png")


def load_p95(ld):
    groups = ld["groups"]
    labels = [f"{t}\n{c}" for t in ("heavy", "light1", "light2") for c in ("short", "long")]
    fig, ax = plt.subplots(figsize=(7.5, 3.6))
    w = 0.8 / max(1, len(ld["policies"]))
    for i, p in enumerate(ld["policies"]):
        vals, ns = [], []
        for t in ("heavy", "light1", "light2"):
            for c in ("short", "long"):
                g = next(g for g in groups if g["policy"] == p and g["tenant"] == t and g["job_class"] == c)
                vals.append(g["p95"])
                ns.append(g["n"])
        xs = [k + (i - (len(ld["policies"]) - 1) / 2) * w for k in range(len(labels))]
        ax.bar(xs, vals, width=w * 0.92, color=POLICY_COLORS.get(p, S4), label=p)
        for x, v, n in zip(xs, vals, ns):
            ax.text(x, v, f"N={n}", ha="center", va="bottom", fontsize=6, color=INK2, rotation=90)
    ax.set_xticks(range(len(labels)), labels)
    ax.set_ylabel("p95 queue wait (virtual s)")
    ax.set_title("Part 3: p95 wait by user × class, pooled over 3 seeds (N above bars)")
    ax.legend(fontsize=8)
    return _save(fig, "load_wait_p95.png")


def load_share(ld):
    fig, ax = plt.subplots(figsize=(7.5, 2.8))
    rows = [("demand", ld["demand_share"])] + [(p["policy"], p["work_share_burst"]) for p in ld["per_policy"]]
    for i, (name, share) in enumerate(rows):
        left = 0.0
        for t, col in TENANT_COLORS.items():
            v = share.get(t, 0.0)
            ax.barh(i, v, left=left, color=col, edgecolor=SURF, linewidth=2, label=t if i == 0 else None)
            if v > 0.06:
                ax.text(left + v / 2, i, f"{v:.0%}", ha="center", va="center", color="white", fontsize=8)
            left += v
    ax.set_yticks(range(len(rows)), [r[0] for r in rows])
    ax.invert_yaxis()
    ax.set_xlim(0, 1)
    ax.set_xlabel("share of executed step-work in burst window (weights 1:1:1)")
    ax.set_title("Part 3: who got the capacity during overload")
    ax.legend(ncol=3, loc="lower right", fontsize=8, bbox_to_anchor=(1, 1.08))
    return _save(fig, "load_work_share.png")
