from __future__ import annotations

import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


# ---------------------------------------------------------------------------
# 1. Locate the three AWS result files
# ---------------------------------------------------------------------------
DATA_DIR = Path("/content") if Path("/content").exists() else Path(".")

HOURLY_FILE = DATA_DIR / "pandas_full_year_hourly.csv"
MONTHLY_FILE = DATA_DIR / "monthly_trend.csv"
PERFORMANCE_FILE = DATA_DIR / "performance_summary.csv"
OUTPUT_DIR = Path("report_figures")
OUTPUT_DIR.mkdir(exist_ok=True)
for existing_figure in OUTPUT_DIR.glob("figure_*.png"):
    existing_figure.unlink()

required = [HOURLY_FILE, MONTHLY_FILE, PERFORMANCE_FILE]
missing = [str(path) for path in required if not path.exists()]
if missing:
    raise FileNotFoundError(
        "Cannot find the following required file(s):\n- " + "\n- ".join(missing)
        + "\nCheck DATA_DIR near the top of this notebook."
    )

print(f"Reading AWS result files from: {DATA_DIR}")

hourly = pd.read_csv(HOURLY_FILE).sort_values("hour")
monthly = pd.read_csv(MONTHLY_FILE)
performance = pd.read_csv(PERFORMANCE_FILE)

month_column = "Month" if "Month" in monthly.columns else "month"
monthly = monthly.sort_values(month_column)


# ---------------------------------------------------------------------------
# Shared visual style
# ---------------------------------------------------------------------------
NAVY = "#203748"
BLUE = "#2E74B5"
RED = "#C62828"

plt.rcParams.update(
    {
        "font.size": 10,
        "axes.titlesize": 12,
        "axes.labelsize": 10,
        "figure.dpi": 120,
        "savefig.dpi": 220,
    }
)


# ---------------------------------------------------------------------------
# Figure 2: hourly delay results in two separate panels
# ---------------------------------------------------------------------------
hours = hourly["hour"].astype(int).to_numpy()
avg_delay = hourly["avg_arr_delay"].astype(float).to_numpy()
pct_delay = hourly["pct_delayed15"].astype(float).to_numpy()

fig, (ax1, ax2) = plt.subplots(
    2, 1, figsize=(10.2, 7.0), sharex=True, gridspec_kw={"hspace": 0.18}
)

ax1.plot(hours, avg_delay, marker="o", color=BLUE, linewidth=2.3)
ax1.set_ylabel("Average arrival delay (minutes)", color=BLUE)
ax1.set_title("(a) Average arrival delay", loc="left", color=NAVY, weight="bold")

ax2.plot(hours, pct_delay, marker="s", color=RED, linewidth=2.0)
ax2.set_ylabel("Flights delayed at least 15 minutes (%)", color=RED)
ax2.set_title("(b) Delayed-15 percentage", loc="left", color=NAVY, weight="bold")
ax2.set_xticks(range(0, 24, 2))
ax2.set_xlabel("Scheduled departure hour")

fig.suptitle("Delay accumulation across the operating day", fontsize=14, color=NAVY, weight="bold")
fig.subplots_adjust(left=0.12, right=0.98, top=0.88, bottom=0.09, hspace=0.25)
fig.savefig(OUTPUT_DIR / "figure_2_hourly_delay.png", bbox_inches="tight", facecolor="white")
plt.show()
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 3: monthly delay trend in two separate panels
# ---------------------------------------------------------------------------
months = monthly[month_column].astype(int).to_numpy()
monthly_avg = monthly["avg_arr_delay"].astype(float).to_numpy()
monthly_pct = monthly["pct_delayed15"].astype(float).to_numpy()

fig, (ax1, ax2) = plt.subplots(
    2, 1, figsize=(10.2, 7.0), sharex=True, gridspec_kw={"hspace": 0.18}
)
ax1.plot(months, monthly_avg, marker="o", color=BLUE, linewidth=2.3)
ax1.set_ylabel("Average arrival delay (minutes)", color=BLUE)
ax1.set_title("(a) Average arrival delay", loc="left", color=NAVY, weight="bold")

ax2.plot(months, monthly_pct, marker="s", color=RED, linewidth=2.0)
ax2.set_ylabel("Flights delayed at least 15 minutes (%)", color=RED)
ax2.set_title("(b) Delayed-15 percentage", loc="left", color=NAVY, weight="bold")
ax2.set_xticks(months)
ax2.set_xlabel("Month of 2024")

fig.suptitle("Seasonality of US flight delays in 2024", fontsize=14, color=NAVY, weight="bold")
fig.subplots_adjust(left=0.12, right=0.98, top=0.88, bottom=0.09, hspace=0.25)
fig.savefig(OUTPUT_DIR / "figure_3_monthly_trend.png", bbox_inches="tight", facecolor="white")
plt.show()
plt.close(fig)


# ---------------------------------------------------------------------------
# Figure 4: like-for-like performance comparison
# ---------------------------------------------------------------------------
display_order = ["pandas", "Spark on YARN", "Hadoop Streaming"]
performance["method"] = pd.Categorical(performance["method"], display_order, ordered=True)
performance = performance.sort_values("method")

labels = ["pandas", "Spark\non YARN", "Hadoop\nStreaming"]
wall_times = performance["wall_time_seconds"].astype(float).to_numpy()
colours = [RED, BLUE, NAVY]

fig, ax = plt.subplots(figsize=(9.8, 4.8))
bars = ax.bar(labels, wall_times, color=colours, width=0.62)
ax.set_ylabel("End-to-end wall time (seconds)")
ax.set_title("Like-for-like full-year hourly benchmark", color=NAVY, weight="bold")
ax.set_ylim(0, max(wall_times) * 1.15)

for bar, value in zip(bars, wall_times):
    ax.text(bar.get_x() + bar.get_width() / 2, value + 2.0,
            f"{value:.2f} s", ha="center", weight="bold")

internal = performance["internal_time_seconds"].to_numpy()
for index, value in enumerate(internal):
    if pd.notna(value):
        ax.text(index, wall_times[index] * 0.72, f"Internal: {float(value):.2f} s",
                ha="center", color="white", fontsize=9, weight="bold")

fig.tight_layout()
fig.savefig(OUTPUT_DIR / "figure_4_performance.png", bbox_inches="tight", facecolor="white")
plt.show()
plt.close(fig)


# ---------------------------------------------------------------------------
# Package the three PNG files
# ---------------------------------------------------------------------------
archive = shutil.make_archive("IST3134_report_figures", "zip", OUTPUT_DIR)
print(f"Created three figures in: {OUTPUT_DIR.resolve()}")
print(f"Created ZIP file: {Path(archive).resolve()}")
