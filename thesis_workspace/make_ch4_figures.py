"""Ve bieu do Chuong 4 truc tiep tu results/logs/*.csv (dot chinh thuc).

Bang mau: palette mac dinh cua skill dataviz, thu tu co dinh, da chay validate_palette.js
(light, nen #ffffff): PASS; xanh ngoc 2.82:1 -> co chu giai + hoa van + bang so lieu kem theo.

Cach dung: python make_ch4_figures.py <repo_root> <thu_muc_hinh>
"""
import csv
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import FuncFormatter

ROOT = Path(sys.argv[1])
OUT = Path(sys.argv[2])
LOGS = ROOT / "results" / "logs"

BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, GRID = "#0b0b0b", "#52514e", "#d9d8d4"
STRATS = [("batch_only", "batch_only", BLUE, ""), ("speed_only", "speed_only", ORANGE, "////"),
          ("lambda", "lambda (Query Merger)", AQUA, "\\\\\\\\")]
VN = FuncFormatter(lambda v, _: f"{v:g}".replace(".", ","))  # dau phay thap phan


def vn(x, digits):
    return f"{x:.{digits}f}".replace(".", ",")


plt.rcParams.update({
    "font.family": "Times New Roman", "font.size": 11, "axes.edgecolor": INK2,
    "axes.labelcolor": INK, "xtick.color": INK2, "ytick.color": INK2,
    "axes.spines.top": False, "axes.spines.right": False, "savefig.dpi": 300,
    "hatch.linewidth": 0.6,
})


def read(name, pred):
    with open(LOGS / name, encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if pred(r)]


def style_axis(ax, x_numeric=False):
    ax.yaxis.set_major_formatter(VN)
    if x_numeric:
        ax.xaxis.set_major_formatter(VN)
    ax.yaxis.grid(True, color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


# ---- Hinh 4.1: phan phoi do tre theo loai truy van va chien luoc (BTCUSDT, 2026-11-11) ----
lat = read("bench_latency.csv", lambda r: r["ts"].startswith("2026-11-11T18") and r["symbol"] == "BTCUSDT")
queries = [("Q1_historical", "Q1 – lịch sử"), ("Q2_realtime", "Q2 – 5 phút gần nhất"),
           ("Q3_hybrid", "Q3 – 30 phút gần nhất")]
fig, axes = plt.subplots(1, 3, figsize=(7.2, 3.4), sharey=True)
rng = np.random.default_rng(0)
for ax, (q, qlabel) in zip(axes, queries):
    for i, (key, _, color, hatch) in enumerate(STRATS):
        vals = np.array([float(r["latency_ms"]) for r in lat if r["query_type"] == q and r["strategy"] == key])
        bp = ax.boxplot(vals, positions=[i], widths=0.55, patch_artist=True, showfliers=False,
                        medianprops=dict(color=INK, linewidth=1.4),
                        whiskerprops=dict(color=INK2, linewidth=0.9), capprops=dict(color=INK2, linewidth=0.9))
        for box in bp["boxes"]:
            box.set(facecolor=color, alpha=0.85, edgecolor="white", linewidth=1.2, hatch=hatch)
        ax.scatter(i + rng.uniform(-0.18, 0.18, len(vals)), vals, s=9, color=INK, alpha=0.45,
                   linewidths=0, zorder=3)
    ax.set_title(qlabel, fontsize=11, color=INK)
    ax.set_xticks([0, 1, 2], ["batch", "speed", "lambda"])
    style_axis(ax)
axes[0].set_ylabel("Độ trễ truy vấn (ms)")
handles = [plt.Rectangle((0, 0), 1, 1, facecolor=c, hatch=h, edgecolor="white") for _, _, c, h in STRATS]
fig.legend(handles, [lbl for _, lbl, _, _ in STRATS], loc="lower center", ncol=3, frameon=False, fontsize=10)
fig.tight_layout(rect=(0, 0.08, 1, 1))
fig.savefig(OUT / "hinh_4_1.png")
plt.close(fig)

# ---- Hinh 4.2: do tre doc tung lan truoc va sau gop tep (2026-11-11, mode=real) ----
comp = read("bench_compaction.csv", lambda r: r["mode"] == "real" and r["ts"].startswith("2026-11-11T19:02"))
fig, ax = plt.subplots(figsize=(7.2, 3.3))
for phase, label, color, marker, dy in (("before", "Trước gộp tệp (118 tệp)", BLUE, "o", 10),
                                        ("after", "Sau gộp tệp (10 tệp)", ORANGE, "s", -10)):
    vals = [float(r["read_latency_ms"]) / 1000 for r in comp if r["phase"] == phase]
    ax.plot(range(1, len(vals) + 1), vals, color=color, linewidth=2, marker=marker, markersize=6,
            markeredgecolor="white", markeredgewidth=1.2, label=label)
    ax.annotate(f"trung vị {vn(np.median(vals[1:]), 2)} s", xy=(len(vals), vals[-1]),
                xytext=(8, dy), textcoords="offset points", va="center", fontsize=9.5, color=INK2)
ax.annotate("lần đọc lạnh đầu tiên: 8,74 s", xy=(1, 8.7405), xytext=(10, -2), textcoords="offset points",
            va="center", fontsize=9.5, color=INK2)
ax.set_xlabel("Lần đọc thứ")
ax.set_ylabel("Độ trễ đọc (giây)")
ax.set_xticks(range(1, 21, 2))
ax.set_xlim(0.5, 23.5)
ax.set_ylim(0, 9.5)
style_axis(ax)
ax.legend(frameon=False, loc="upper right", fontsize=10)
fig.tight_layout()
fig.savefig(OUT / "hinh_4_2.png")
plt.close(fig)

# ---- Hinh 4.3: phan phoi sai so VWAP tung cua so (2026-11-11) ----
rep = read("bench_reprocess.csv", lambda r: r["ts"].startswith("2026-11-11T19") and r["pct_error"])
fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.0), sharey=True)
bins = np.linspace(0, 0.35, 15)
for ax, (sym, color, hatch) in zip(axes, (("BTCUSDT", BLUE, ""), ("ETHUSDT", ORANGE, "////"))):
    vals = np.array([float(r["pct_error"]) for r in rep if r["symbol"] == sym])
    ax.hist(vals, bins=bins, color=color, edgecolor="white", linewidth=1.2, hatch=hatch)
    ax.axvline(vals.mean(), color=INK, linewidth=1.2, linestyle="--")
    ax.text(0.97, 0.95, f"MAPE = {vn(vals.mean(), 4)}%\n(đường nét đứt)", transform=ax.transAxes,
            ha="right", va="top", fontsize=9.5, color=INK)
    ax.set_title(f"{sym} ({len(vals)} cửa sổ)", fontsize=11, color=INK)
    ax.set_xlabel("Sai số VWAP tương đối (%)")
    style_axis(ax, x_numeric=True)
axes[0].set_ylabel("Số cửa sổ")
fig.tight_layout()
fig.savefig(OUT / "hinh_4_3.png")
plt.close(fig)
print("ok")
