"""
Lakehouse telemetry and audit logging utility.
"""
import time
from typing import Any, Dict


class LakehouseTelemetry:
    """
    Collects run statistics across Bronze, Silver, Gold, and Quarantine layers
    and outputs a standardized data engineering audit report.
    """

    def __init__(self, job_name: str = "lakehouse_etl"):
        self.job_name = job_name
        self.start_time = time.time()
        self.end_time = None
        self.metrics: Dict[str, Any] = {}

    def record_stage(self, stage_name: str, stage_metrics: Dict[str, Any]) -> None:
        """Records metrics from a pipeline stage."""
        self.metrics[stage_name] = stage_metrics

    def finalize(self) -> Dict[str, Any]:
        """Calculates total elapsed time and formats final report."""
        self.end_time = time.time()
        elapsed = round(self.end_time - self.start_time, 2)
        summary = {
            "job_name": self.job_name,
            "elapsed_seconds": elapsed,
            "stages": self.metrics,
        }
        return summary

    def format_cli_summary(self, summary: Dict[str, Any]) -> str:
        """Formats summary as a clean console table for MSc grading and monitoring."""
        lines = [
            "=" * 70,
            f"LAKEHOUSE ETL PIPELINE RUN SUMMARY ({summary.get('job_name')})",
            "=" * 70,
            f"  Total Duration: {summary.get('elapsed_seconds')}s",
        ]

        stages = summary.get("stages", {})
        if "extract" in stages:
            ext = stages["extract"]
            lines.append("\n[EXTRACT]")
            lines.append(f"  Input Source:     {ext.get('source')}")
            lines.append(f"  Extracted Rows:   {ext.get('records_read', 0):,}")

        if "bronze" in stages:
            brz = stages["bronze"]
            lines.append("\n[BRONZE LAYER]")
            lines.append(f"  Destination:      {brz.get('destination')}")
            lines.append(f"  Written Records:  {brz.get('records_written', 0):,}")
            lines.append(f"  Write Mode:       {brz.get('mode')}")

        if "silver" in stages:
            slv = stages["silver"]
            lines.append("\n[SILVER LAYER]")
            lines.append(f"  Valid Records:    {slv.get('silver_records', 0):,}")
            lines.append(f"  Quarantined:      {slv.get('quarantined_records', 0):,}")
            lines.append(f"  Duplicates Dropped:{slv.get('duplicates_removed', 0):,}")
            lines.append(f"  Silver Storage:   {slv.get('silver_destination')}")
            lines.append(f"  Quarantine Storage:{slv.get('quarantine_destination')}")

        if "gold" in stages:
            gld = stages["gold"]
            lines.append("\n[GOLD LAYER - DOWNSTREAM ANALYTICS & ML]")
            lines.append(f"  Enriched Features: {gld.get('enriched_transactions_count', 0):,} rows")
            lines.append(f"  Customer Profiles: {gld.get('customer_aggregates_count', 0):,} accounts")
            lines.append(f"  Hourly Rollups:    {gld.get('hourly_aggregates_count', 0):,} hours")
            lines.append(f"  Enriched Path:     {gld.get('enriched_path')}")

        lines.append("=" * 70)
        return "\n".join(lines)
