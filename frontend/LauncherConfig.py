from dataclasses import dataclass
import json
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).parent.parent
CONFIG_DIR = WORKSPACE_ROOT / "frontend" / "launcher_configs"
CONFIG_DIR.mkdir(exist_ok=True)

@dataclass
class LauncherConfig:
    screen_file: str
    bot_script: str
    screener_script: str
    screener_output: str
    api_key_id: str
    private_key_path: str
    subaccount: str
    max_bots: int
    yes_budget_cents: int
    no_budget_cents: int
    launch_delay_seconds: float
    refresh_interval_seconds: float
    poll_seconds: float
    minimum_carryover_value_cents: float
    run_screener_on_start: bool
    use_demo: bool
    dry_run: bool
    host: str
    api_prefix: str
    status: str
    mve_filter: str
    max_markets_to_scan: int
    top_n: int
    min_spread_cents: int
    max_spread_cents: int
    min_yes_bid_cents: int
    min_no_bid_cents: int
    min_vol24h: float
    min_oi: float
    excluded_series: list
    min_time_to_close_hrs: float
    max_time_to_close_hrs: float
    default_tick_cents: int
    quote_size: int
    minimum_expected_edge_cents_to_quote: float
    default_toxicity_cents: float
    maker_fee_factor: float
    fair_value_mid_weight: float
    fair_value_last_weight: float
    fair_value_max_orderbook_imbalance_adjust_cents: float
    passive_offset_ticks_when_not_improving: int
    candidate_price_levels_to_scan: int
    queue_penalty_cap_cents: float
    queue_penalty_contracts_per_cent: float
    imbalance_toxicity_extra_cents: float
    net_position_contracts: float
    inventory_skew_contracts_per_tick: float
    maximum_inventory_skew_ticks: float

def loadLauncherConfig(name: str) -> LauncherConfig:
    path = CONFIG_DIR / f"{name}.json"
    if path.exists():
        with open(path, 'r') as f:
            data = json.load(f)
        return LauncherConfig(**data)
    else:
        raise FileNotFoundError(f"Configuration file not found: {path}")

def saveLauncherConfig(config: LauncherConfig, name: str) -> bool:
    if not name.strip():
        return False

    path = CONFIG_DIR / f"{name}.json"
    with open(path, 'w') as f:
        json.dump(config.__dict__, f, indent=4)
    return True