#!/usr/bin/env python3
"""BlackRoad OS - VictoriaMetrics-compatible time-series metrics store with SQLite backend."""

from __future__ import annotations

import json
import math
import re
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DB_PATH = Path.home() / ".blackroad" / "victoriametrics.db"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DataPoint:
    timestamp: float
    value: float


@dataclass
class TimeSeries:
    metric: str
    labels: dict[str, str]
    points: list[DataPoint] = field(default_factory=list)

    @property
    def label_key(self) -> str:
        return json.dumps(self.labels, sort_keys=True)


# ---------------------------------------------------------------------------
# MetricsStore
# ---------------------------------------------------------------------------

class MetricsStore:
    """SQLite-backed time-series store with VictoriaMetrics-compatible API."""

    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_db()
        self._seed_metrics()

    def _init_db(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS metric_series (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metric TEXT NOT NULL,
                labels TEXT NOT NULL DEFAULT '{}',
                UNIQUE(metric, labels)
            );
            CREATE TABLE IF NOT EXISTS data_points (
                series_id INTEGER NOT NULL,
                ts REAL NOT NULL,
                value REAL NOT NULL,
                FOREIGN KEY(series_id) REFERENCES metric_series(id)
            );
            CREATE INDEX IF NOT EXISTS idx_dp_series_ts ON data_points(series_id, ts);
        """)
        self._conn.commit()

    def _seed_metrics(self) -> None:
        """Pre-populate demo metrics."""
        now = time.time()
        seed: list[tuple[str, dict, list[tuple[float, float]]]] = [
            ("worlds_total", {"env": "prod"}, [(now - i * 60, 1000 + i * 10) for i in range(10)]),
            ("cpu_usage", {"host": "blackroad-pi", "cpu": "total"},
             [(now - i * 60, 30.0 + 20.0 * math.sin(i * 0.5)) for i in range(60)]),
            ("memory_bytes", {"host": "blackroad-pi", "type": "used"},
             [(now - i * 60, 512_000_000 + i * 1_000_000) for i in range(60)]),
            ("api_latency_ms", {"service": "gateway", "method": "POST"},
             [(now - i * 60, 50.0 + 30.0 * math.sin(i * 0.3)) for i in range(60)]),
        ]
        for metric, labels, points in seed:
            series_id = self._get_or_create_series(metric, labels)
            existing = self._conn.execute(
                "SELECT COUNT(*) FROM data_points WHERE series_id=?", (series_id,)
            ).fetchone()[0]
            if existing == 0:
                self._conn.executemany(
                    "INSERT INTO data_points (series_id, ts, value) VALUES (?,?,?)",
                    [(series_id, ts, val) for ts, val in points],
                )
        self._conn.commit()

    def _get_or_create_series(self, metric: str, labels: dict) -> int:
        labels_json = json.dumps(labels, sort_keys=True)
        row = self._conn.execute(
            "SELECT id FROM metric_series WHERE metric=? AND labels=?", (metric, labels_json)
        ).fetchone()
        if row:
            return row["id"]
        cur = self._conn.execute(
            "INSERT INTO metric_series (metric, labels) VALUES (?,?)", (metric, labels_json)
        )
        self._conn.commit()
        return cur.lastrowid

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, metric: str, value: float, labels: dict | None = None,
              timestamp: float | None = None) -> None:
        labels = labels or {}
        ts = timestamp or time.time()
        series_id = self._get_or_create_series(metric, labels)
        self._conn.execute(
            "INSERT INTO data_points (series_id, ts, value) VALUES (?,?,?)",
            (series_id, ts, value),
        )
        self._conn.commit()

    def write_batch(self, points: list[dict]) -> int:
        written = 0
        for p in points:
            self.write(
                p["metric"], p["value"],
                labels=p.get("labels", {}),
                timestamp=p.get("timestamp"),
            )
            written += 1
        return written

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def query(self, metric: str, start: float | None = None, end: float | None = None,
              labels: dict | None = None, step: int = 0) -> list[TimeSeries]:
        rows = self._conn.execute(
            "SELECT id, labels FROM metric_series WHERE metric=?", (metric,)
        ).fetchall()
        results = []
        for row in rows:
            series_labels = json.loads(row["labels"])
            if labels and not all(series_labels.get(k) == v for k, v in labels.items()):
                continue
            clauses = ["series_id=?"]
            params: list[Any] = [row["id"]]
            if start:
                clauses.append("ts>=?"); params.append(start)
            if end:
                clauses.append("ts<=?"); params.append(end)
            pts = self._conn.execute(
                f"SELECT ts, value FROM data_points WHERE {' AND '.join(clauses)} ORDER BY ts",
                params,
            ).fetchall()
            points = [DataPoint(r["ts"], r["value"]) for r in pts]
            results.append(TimeSeries(metric=metric, labels=series_labels, points=points))
        return results

    def query_instant(self, metric: str, timestamp: float | None = None,
                      labels: dict | None = None) -> list[tuple[dict, float]]:
        ts = timestamp or time.time()
        series_list = self.query(metric, start=ts - 300, end=ts, labels=labels)
        results = []
        for s in series_list:
            if s.points:
                results.append((s.labels, s.points[-1].value))
        return results

    def query_promql(self, expr: str, start: float | None = None,
                     end: float | None = None) -> list[TimeSeries]:
        """Parse a basic PromQL metric selector like metric_name{k="v",...}[5m]."""
        m = re.match(r'(\w+)(\{[^}]*\})?(\[\d+[smh]\])?', expr.strip())
        if not m:
            return []
        metric = m.group(1)
        labels: dict[str, str] = {}
        if m.group(2):
            for pair in re.finditer(r'(\w+)="([^"]*)"', m.group(2)):
                labels[pair.group(1)] = pair.group(2)
        # range offset
        if m.group(3) and start is None:
            range_str = m.group(3)[1:-1]
            mult = {"s": 1, "m": 60, "h": 3600}.get(range_str[-1], 1)
            seconds = int(range_str[:-1]) * mult
            start = time.time() - seconds
        return self.query(metric, start=start, end=end, labels=labels or None)

    # ------------------------------------------------------------------
    # Meta
    # ------------------------------------------------------------------

    def get_metric_names(self) -> list[str]:
        rows = self._conn.execute("SELECT DISTINCT metric FROM metric_series ORDER BY metric").fetchall()
        return [r["metric"] for r in rows]

    def get_label_values(self, label_name: str) -> list[str]:
        rows = self._conn.execute("SELECT DISTINCT labels FROM metric_series").fetchall()
        values: set[str] = set()
        for r in rows:
            lbls = json.loads(r["labels"])
            if label_name in lbls:
                values.add(lbls[label_name])
        return sorted(values)

    def downsample(self, metric: str, factor: int = 10) -> int:
        """Thin out data points by keeping every Nth point per series."""
        series = self.query(metric)
        removed = 0
        for s in series:
            series_id = self._get_or_create_series(metric, s.labels)
            pts = self._conn.execute(
                "SELECT rowid, ts FROM data_points WHERE series_id=? ORDER BY ts",
                (series_id,),
            ).fetchall()
            to_delete = [pts[i]["rowid"] for i in range(len(pts)) if i % factor != 0]
            if to_delete:
                self._conn.executemany(
                    "DELETE FROM data_points WHERE rowid=?", [(r,) for r in to_delete]
                )
                removed += len(to_delete)
        self._conn.commit()
        return removed

    def get_stats(self) -> dict:
        series_count = self._conn.execute("SELECT COUNT(*) FROM metric_series").fetchone()[0]
        point_count = self._conn.execute("SELECT COUNT(*) FROM data_points").fetchone()[0]
        metric_names = self.get_metric_names()
        return {
            "series": series_count,
            "data_points": point_count,
            "metrics": metric_names,
            "db_size_bytes": self.db_path.stat().st_size if self.db_path.exists() else 0,
        }

    def export_prometheus(self, metric: str | None = None) -> str:
        """Export metrics in Prometheus text format."""
        names = [metric] if metric else self.get_metric_names()
        lines: list[str] = []
        for name in names:
            series_list = self.query(name)
            for s in series_list:
                if not s.points:
                    continue
                lbl_str = ",".join(f'{k}="{v}"' for k, v in s.labels.items())
                label_part = f"{{{lbl_str}}}" if lbl_str else ""
                pt = s.points[-1]
                lines.append(f"{name}{label_part} {pt.value} {int(pt.timestamp * 1000)}")
        return "\n".join(lines) + "\n"

    def retention_sweep(self, max_age_seconds: float = 86400 * 30) -> int:
        cutoff = time.time() - max_age_seconds
        cur = self._conn.execute("DELETE FROM data_points WHERE ts < ?", (cutoff,))
        self._conn.commit()
        return cur.rowcount


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="BlackRoad OS Metrics Store")
    sub = parser.add_subparsers(dest="cmd")

    p_write = sub.add_parser("write", help="Write a data point")
    p_write.add_argument("metric")
    p_write.add_argument("value", type=float)
    p_write.add_argument("--labels", default="{}", help='JSON labels')

    p_query = sub.add_parser("query", help="Query a metric")
    p_query.add_argument("metric")
    p_query.add_argument("--range", type=int, default=300, help="Seconds to look back")

    p_prom = sub.add_parser("prometheus", help="Export Prometheus format")
    p_prom.add_argument("metric", nargs="?", default=None)

    args = parser.parse_args()
    store = MetricsStore()

    if args.cmd == "write":
        labels = json.loads(args.labels)
        store.write(args.metric, args.value, labels=labels)
        print(f"Written {args.metric}={args.value}")
    elif args.cmd == "query":
        start = time.time() - args.range
        series_list = store.query(args.metric, start=start)
        for s in series_list:
            print(f"\n{s.metric}{s.labels}  ({len(s.points)} points)")
            for pt in s.points[-5:]:
                print(f"  {pt.timestamp:.0f}  {pt.value:.4f}")
    elif args.cmd == "prometheus":
        print(store.export_prometheus(args.metric), end="")
    else:
        stats = store.get_stats()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
