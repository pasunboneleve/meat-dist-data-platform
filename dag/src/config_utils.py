from datetime import UTC, date, datetime, timedelta

from airflow.sdk import Asset, Context, get_current_context


def get_target_config(
    context: dict,
    upstream_asset: Asset,
    date_offset_days: int = 1,
    metadata_key: str = "target_date_str",
) -> dict[str, str]:
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
            print("DEBUG: Full latest_event:", latest_event.__dict__)
            print("DEBUG: latest_event.extra:", latest_event.extra)
            print(
                "DEBUG: latest_event.metadata (fallback):",
                getattr(latest_event, "metadata", None),
            )
            print("DEBUG: All event keys:", dir(latest_event))

            extra = latest_event.extra or {}
            metadata = getattr(latest_event, "metadata", {}) or {}
            target_date_str = (
                extra.get("target_date_str")
                or metadata.get("target_date_str")
                or extra.get("target_date")  # possible typo variants
                or metadata.get("target_date")
            )

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
                    f"Upstream event missing 'target_date_str' in extra or metadata. "
                    f"Extra: {extra}, Metadata: {metadata}"
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


def generate_dataproc_batch_id(
    prefix: str = "bronze-to-silver-dv2",
    context: Context | None = None,
    max_length: int = 63,
) -> str:
    """
    Generates a valid Dataproc batch_id that conforms to:
    - Pattern: [a-z0-9][a-z0-9\\-]{2,61}[a-z0-9]
    - Only lowercase letters, numbers, and dashes
    - Starts and ends with letter or number
    - Length: up to 63 characters (Dataproc limit)

    Uses runtime context values (ds_nodash, ts_nodash, try_number).

    Args:
        prefix: Optional custom prefix (default: "bronze-to-silver-dv2")
        context: Airflow context dict (if None, fetches via get_current_context)
        max_length: Maximum allowed length (default 63)

    Returns:
        A valid, lowercase batch_id string
    """
    if context is None:
        context = get_current_context()

    # Extract rendered macro values
    ds_nodash = context.get("ds_nodash", datetime.now(UTC).strftime("%Y%m%d"))
    ts_nodash = context.get("ts_nodash", datetime.now(UTC).strftime("%Y%m%d%H%M%S"))
    ts_clean = ts_nodash.replace("T", "").replace("+", "-").lower()
    try_number = context["ti"].try_number if "ti" in context else 1

    # Build base ID
    batch_id = f"{prefix}-{ds_nodash}-{ts_clean}-try{try_number}"

    # Ensure lowercase (already should be, but enforce)
    batch_id = batch_id.lower()

    # Truncate if too long (keep suffix for uniqueness)
    if len(batch_id) > max_length:
        suffix = f"-try{try_number}"
        available = max_length - len(suffix)
        batch_id = batch_id[:available] + suffix

    # Final validation (should always pass, but defensive)
    if not batch_id[0].isalnum() or not batch_id[-1].isalnum():
        raise ValueError(
            f"Generated batch_id does not start/end with alphanumeric: {batch_id}"
        )

    print(f"Generated Dataproc batch_id: {batch_id} (length: {len(batch_id)})")

    return batch_id
