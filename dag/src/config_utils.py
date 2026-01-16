from datetime import date, timedelta
from typing import Dict

from airflow.sdk import Asset


def get_target_config(
    context: Dict,
    upstream_asset: Asset,
    date_offset_days: int = 1,
    metadata_key: str = "target_date_str",
) -> Dict[str, str]:
    """
    Reusable function to resolve target_date_str and prefix.

    Works for:
    - Asset-triggered runs (reads from upstream asset event metadata)
    - Manual triggers (prefers explicit logical_date if set)

    Args:
        context: The Airflow task context
        upstream_asset: The Asset object that triggers this DAG
        date_offset_days: How many days to subtract from logical_date (default 1)
        metadata_key: The extra/metadata key where upstream stores the date string

    Returns:
        Dict with 'target_date_str' and 'target_prefix'
    """
    triggering_events = context.get("triggering_asset_events", {})

    if triggering_events and upstream_asset in triggering_events:
        # Asset-triggered path – prefer metadata from upstream
        events = triggering_events[upstream_asset]
        if events:
            latest_event = events[-1]
            extra = latest_event.extra or {}
            target_date_str = extra.get(metadata_key)

            if target_date_str:
                try:
                    target_date = date.fromisoformat(target_date_str)
                    print(
                        f"Asset-triggered: Using upstream metadata '{metadata_key}': {target_date_str}"
                    )
                except ValueError as e:
                    raise ValueError(
                        f"Invalid date format in upstream metadata '{metadata_key}': {target_date_str}"
                    ) from e
            else:
                raise ValueError(
                    f"Upstream asset event missing '{metadata_key}' in extra. "
                    f"Check producer DAG is emitting metadata correctly."
                )
        else:
            raise ValueError(f"Empty event list for asset {upstream_asset.uri}")
    else:
        # Manual trigger path – try logical_date first
        logical_date = context.get("logical_date")

        if logical_date is not None:
            target_date = logical_date.date() - timedelta(days=date_offset_days)
            print(
                f"Manual trigger: Using logical_date - {date_offset_days} day(s): "
                f"{target_date.isoformat()}"
            )
        else:
            # Very rare fallback for manual runs without logical_date
            dag_run = context["dag_run"]
            fallback_ts = dag_run.queued_at or dag_run.created_at
            target_date = (fallback_ts - timedelta(days=date_offset_days)).date()
            print(
                f"Manual trigger without logical_date: Using fallback date "
                f"{target_date.isoformat()}"
            )

    target_date_str = target_date.isoformat()

    # Rebuild prefix (assumes Hive-style partitioning – adjust if needed)
    prefix = (
        f"carcasses/year={target_date.year}/"
        f"month={target_date.month:02d}/"
        f"day={target_date.day:02d}/"
    )

    print(f"Resolved target_date_str: {target_date_str}")
    print(f"Resolved prefix: {prefix}")

    return {
        "target_date_str": target_date_str,
        "target_prefix": prefix,
    }
