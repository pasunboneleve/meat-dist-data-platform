from datetime import UTC, datetime

from airflow.models.xcom import XComApi
from airflow.sdk import Asset, Context, get_current_context


def get_config_from_trigger(
    context: dict,
    upstream_asset: Asset,
) -> dict[str, str]:
    """
    Resolves run configuration by pulling from an upstream XCom for asset-
    triggered runs or using the logical date for manual runs.

    Args:
        context: The Airflow task context.
        upstream_asset: The asset that is expected to trigger this DAG.

    Returns:
        A dictionary containing the configuration for the run.
    """
    triggering_events = context.get("triggering_asset_events", {})

    if triggering_events and upstream_asset in triggering_events:
        # Asset-triggered: Pull the XCom pushed by the producer DAG
        latest_event = triggering_events[upstream_asset][-1]
        source_dag_id = latest_event.source_dag_id
        source_run_id = latest_event.source_run_id
        source_task_id = latest_event.source_task_id

        print(
            f"Asset-triggered run. Pulling XCom from {source_dag_id}.{source_task_id} run {source_run_id}"
        )
        config = XComApi.get_one(
            key="asset_config",
            dag_id=source_dag_id,
            run_id=source_run_id,
            task_id=source_task_id,
        )
        if not config:
            raise ValueError(
                f"Could not find XCom 'asset_config' from {source_dag_id} run {source_run_id}. "
                "Ensure the producer DAG's final task is pushing this XCom."
            )
        print(f"Successfully pulled config: {config}")
        return config
    else:
        # Manual trigger: Fallback to using logical_date
        print("Manual trigger run. Building config from logical_date.")
        logical_date = context["logical_date"]
        target_date = logical_date.date()
        target_date_str = target_date.isoformat()
        prefix = (
            f"carcasses/year={target_date.year}/"
            f"month={target_date.month:02d}/"
            f"day={target_date.day:02d}/"
        )
        config = {
            "target_date_str": target_date_str,
            "target_prefix": prefix,
        }
        print(f"Built manual config: {config}")
        return config


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
