from dataclasses import dataclass, field
import json
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).parent.parent
CONFIG_DIR = WORKSPACE_ROOT / "frontend" / "launcher_configs"
CONFIG_DIR.mkdir(exist_ok=True)

@dataclass
class LauncherConfig:
    screen_file: str = ""
    bot_script: str = ""
    screener_script: str = ""
    screener_output: str = ""
    api_key_id: str = ""
    private_key_path: str = ""
    subaccount: str = ""
    max_bots: int = 0
    yes_budget_cents: int = 0
    no_budget_cents: int = 0
    launch_delay_seconds: float = 0.0
    refresh_interval_seconds: float = 0.0
    poll_seconds: float = 0.0
    minimum_carryover_value_cents: float = 0.0
    run_screener_on_start: bool = False
    use_demo: bool = False
    dry_run: bool = False
    host: str = ""
    api_prefix: str = ""
    status: str = ""
    mve_filter: str = ""
    max_markets_to_scan: int = 0
    top_n: int = 0
    min_spread_cents: int = 0
    max_spread_cents: int = 0
    min_yes_bid_cents: int = 0
    min_no_bid_cents: int = 0
    min_vol24h: float = 0.0
    min_oi: float = 0.0
    excluded_series: list = field(default_factory=list)
    min_time_to_close_hrs: float = 0.0
    max_time_to_close_hrs: float = 0.0
    default_tick_cents: int = 0
    quote_size: int = 0
    minimum_expected_edge_cents_to_quote: float = 0.0
    default_toxicity_cents: float = 0.0
    maker_fee_factor: float = 0.0
    fair_value_mid_weight: float = 0.0
    fair_value_last_weight: float = 0.0
    fair_value_max_orderbook_imbalance_adjust_cents: float = 0.0
    passive_offset_ticks_when_not_improving: int = 0
    candidate_price_levels_to_scan: int = 0
    queue_penalty_cap_cents: float = 0.0
    queue_penalty_contracts_per_cent: float = 0.0
    imbalance_toxicity_extra_cents: float = 0.0
    net_position_contracts: float = 0.0
    inventory_skew_contracts_per_tick: float = 0.0
    maximum_inventory_skew_ticks: float = 0.0

    def __post_init__(self):
        if isinstance(self.excluded_series, str):
            self.excluded_series = [s.strip() for s in self.excluded_series.split(',') if s.strip()]

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