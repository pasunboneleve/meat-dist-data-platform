from airflow.sdk import Asset


def emit_asset_with_metadata(
    context: dict, asset: Asset, extra: dict, log_prefix: str = "Emitted asset"
):
    outlet_events = context.get("outlet_events")
    if outlet_events is None:
        print(f"{log_prefix}: outlet_events not in context — skipping metadata")
        return

    dated_asset = Asset(uri=asset.uri, extra=extra)

    if asset in outlet_events:
        outlet_events[asset].add(dated_asset)
        print(f"{log_prefix} with extra: {dated_asset.extra}")
    else:
        print(f"{log_prefix}: {asset.uri} not in outlet_events — metadata skipped")
