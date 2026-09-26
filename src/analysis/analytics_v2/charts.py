"""Optional matplotlib renderer; consumes only the persisted derived tables."""
import io
from pathlib import Path


def render_charts(tables, destination, *, dataset_label):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    from .reporting import write_exact

    plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False})
    dest = Path(destination)
    def save_figure(fig, name):
        fig.tight_layout(rect=(0, .045, 1, 1))
        fig.text(.99, .012, f"Quantia Analytics v2 · {dataset_label} · fuente: tablas del run", ha="right", fontsize=7)
        buffer = io.BytesIO()
        fig.savefig(buffer, format="png", dpi=130, metadata={"Software": "Quantia Analytics v2"})
        plt.close(fig)
        write_exact(dest / f"{name}.png", buffer.getvalue())

    def save(name, title, draw):
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.set_title(title, loc="left", fontsize=14, pad=18)
        if not draw(ax):
            ax.text(.5, .5, "Evidencia insuficiente / no disponible", ha="center", va="center", transform=ax.transAxes)
        ax.grid(alpha=.18)
        save_figure(fig, name)

    def lines(ax, rows, x, y, keys):
        groups = {}
        for row in rows:
            if row.get(y) is not None:
                groups.setdefault(tuple(row[k] for k in keys), []).append(row)
        for key, group in sorted(groups.items()):
            group.sort(key=lambda r: r[x])
            ax.plot([r[x] for r in group], [r[y] for r in group], marker="o", markersize=3, label="/".join(map(str, key)))
        if groups:
            ax.legend(fontsize=7, loc="best")
        ax.set_xlabel(x)
        ax.set_ylabel(y)
        return bool(groups)

    def equity(ax):
        points = [p for p in tables["equity"] if p["reconciled"]]
        # Never overlay accounts or sizing policies as though they were comparable.
        keys = {(p["account_id"], p["currency"], p["sizing_policy"]) for p in points}
        if len(keys) != 1:
            return False
        return lines(ax, points, "at", "nav", ["kind"])

    def histogram(ax):
        rows = [r for r in tables["outlier_analysis"] if r["cost_scenario"] == "RESEARCH_BASE" and r["source_module"] == "CORE" and r["horizon_days"] == 5]
        values = [r["contribution_unit_notional"] for r in rows]
        if values:
            ax.hist(values, bins=min(20, max(3, len(values) // 2)), color="#267786", alpha=.8)
            ax.axvline(np.mean(values), color="#c5503b", label=f"media · n={len(values)}")
            ax.axvline(np.median(values), color="#142b42", linestyle="--", label="mediana")
            ax.legend()
        ax.set_xlabel("Retorno neto BASE, CORE 5D")
        return bool(values)

    def contributors(ax):
        rows = [r for r in tables["outlier_analysis"] if r["cost_scenario"] == "RESEARCH_BASE" and r["source_module"] == "CORE" and r["horizon_days"] == 5]
        rows.sort(key=lambda r: r["contribution_unit_notional"])
        rows = rows if len(rows) <= 20 else rows[:10] + rows[-10:]
        ax.barh(range(len(rows)), [r["contribution_unit_notional"] for r in rows], color=["#267786" if r["contribution_unit_notional"] > 0 else "#c5503b" for r in rows])
        ax.set_yticks(range(len(rows)), [r["ticker"] + "/" + r["episode_id"][:4] for r in rows])
        ax.set_xlabel("Contribución contrafactual a notional unitario; no pesos reales")
        return bool(rows)

    def waterfall(ax):
        rows = [r for r in tables["economic_pnl"] if r["status"] == "COMPLETE"]
        if len(rows) != 1:
            return False
        r = rows[0]
        values = [r["realized_pnl"], r["unrealized_pnl_change"], -r["fees"], -r["taxes"], -r["financing"]-r["other_costs"]]
        ax.bar(range(5), values, bottom=np.r_[0, np.cumsum(values)[:-1]], color=["#267786" if v >= 0 else "#c5503b" for v in values])
        ax.bar(5, r["economic_pnl_net"], color="#142b42")
        ax.set_xticks(range(6), ["Realizado", "Δ no realizado", "Fees", "Impuestos", "Otros", "PnL neto"])
        ax.set_ylabel(r["currency"])
        return True

    def matrix(ax):
        from matplotlib.colors import ListedColormap
        rows = tables["gates"]
        colors = {"CONFIRMED": "#267786", "PROVISIONAL_GUARDED": "#a67715", "FAIL": "#c5503b", "IMMATURE": "#68737d", "DISABLED_SHADOW": "#696094", "OBSERVE": "#68737d"}
        if rows:
            groups = sorted({(r["account_id"], r["source_module"], r["cohort"]) for r in rows})
            horizons = sorted({r["horizon_days"] for r in rows})
            states = list(colors)
            grid = np.full((len(groups), len(horizons)), len(states), dtype=int)
            for row in rows:
                i = groups.index((row["account_id"], row["source_module"], row["cohort"]))
                j = horizons.index(row["horizon_days"])
                grid[i, j] = states.index(row["status"])
                label = row["status"].replace("DISABLED_SHADOW", "DISABLED\nSHADOW").replace("PROVISIONAL_GUARDED", "PROVISIONAL\nGUARDED")
                ax.text(j, i, label, color="white", ha="center", va="center", fontsize=8)
            ax.imshow(grid, cmap=ListedColormap([*colors.values(), "#eeeeee"]), vmin=0, vmax=len(states), aspect="auto")
            ax.set_yticks(range(len(groups)), ["/".join(key[1:]) if len({g[0] for g in groups}) == 1 else "/".join(key) for key in groups])
            ax.set_xticks(range(len(horizons)), [f"{h}D" for h in horizons])
        return bool(rows)

    def costs():
        groups = {}
        for row in tables["cost_sensitivity"]:
            if row["ev_net"] is not None:
                groups.setdefault((row["account_id"], row["source_module"], row["cohort"]), []).append(row)
        if not groups:
            save("cost_sensitivity", "Sensibilidad al costo round-trip", lambda ax: False)
            return
        fig, axes = plt.subplots((len(groups)+1)//2, 2, figsize=(11, 3.1*((len(groups)+1)//2)), squeeze=False)
        from matplotlib.ticker import PercentFormatter
        for ax, (key, rows) in zip(axes.flat, sorted(groups.items())):
            lines(ax, rows, "cost_bps", "ev_net", ["horizon_days"])
            ax.set_title("/".join(key), fontsize=10)
            ax.set_xlabel("Costo round-trip (bps)")
            ax.set_ylabel("EV neto")
            ax.yaxis.set_major_formatter(PercentFormatter(1))
            ax.axhline(0, color="#222222", linewidth=.7)
            ax.grid(alpha=.18)
        for ax in list(axes.flat)[len(groups):]:
            ax.set_visible(False)
        save_figure(fig, "cost_sensitivity")

    save("equity_curve", "NAV reconciliado · misma cuenta, moneda y sizing", equity)
    save("return_histogram", "Distribución de episodios maduros", histogram)
    save("pnl_contributors", "Contribuciones contrafactuales CORE 5D", contributors)
    save("pnl_waterfall", "Reconciliación económica · costos observados", waterfall)
    costs()
    save("signal_decay", "Persistencia por horizonte · BASE 150 bps", lambda ax: lines(ax, tables["signal_decay"], "horizon_days", "ev_net", ["account_id", "source_module", "cohort"]))
    save("gate_matrix", "Gates de auditoría · sin autoridad de capital", matrix)
