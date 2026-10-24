"""Single source of truth for the 4 simulated countries.

Each country is a separate data-residency domain: own UC catalog (pos_<cc>), own S3 bucket,
own pipeline. The legacy POS system formats data differently per country, which is exactly
the "messy legacy data" the silver layer has to translate.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Region:
    code: str
    tz: str
    currency: str
    # strftime format the legacy system uses for TXN_DT, in *store-local* time, no offset.
    legacy_dt_format: str
    # True if the legacy system writes amounts with a decimal comma ("12,50").
    decimal_comma: bool
    stores: int
    registers_per_store: int
    customers: int
    traffic_share: float  # share of total records/minute
    aws_region: str


REGIONS: dict[str, Region] = {
    "PH": Region("PH", "Asia/Manila", "PHP", "%d/%m/%Y %H:%M:%S", False, 40, 6, 20000, 0.30, "ap-southeast-1"),
    "SG": Region("SG", "Asia/Singapore", "SGD", "%Y%m%d%H%M%S", False, 20, 4, 10000, 0.15, "ap-southeast-1"),
    "DE": Region("DE", "Europe/Berlin", "EUR", "%d.%m.%Y %H:%M:%S", True, 30, 5, 15000, 0.20, "eu-central-1"),
    "US": Region("US", "America/New_York", "USD", "%m/%d/%y %I:%M:%S %p", False, 60, 8, 30000, 0.35, "us-east-1"),
}

# Legacy coded fields -> business values (the silver layer owns the real translation; tests assert parity).
TENDER_CODES = {"01": "CASH", "02": "CARD", "03": "EWALLET", "04": "GIFT_CARD"}
STATUS_CODES = {"S": "SALE", "R": "RETURN", "V": "VOID"}

# Static FX to USD for the global aggregated layer (demo only; prod joins a daily FX table).
FX_TO_USD = {"PHP": 0.0175, "SGD": 0.74, "EUR": 1.08, "USD": 1.0}

LEGACY_COLUMNS = [
    "RGN", "STR", "REG", "TXN", "LN", "TXN_DT", "SKU", "QTY", "UNIT_PRC",
    "DISC", "CURR", "TND", "STAT", "LYL", "EMAIL", "UPD_SEQ",
]
