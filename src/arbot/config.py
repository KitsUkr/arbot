from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    telegram_bot_token: str
    telegram_chat_id: str

    poll_interval_seconds: int = 15
    min_profit_threshold: float = 0.01
    min_liquidity_usd: float = 50.0

    title_similarity_threshold: int = 88
    max_expiry_delta_hours: int = 72

    polymarket_fee_bps: int = 0
    predictfun_fee_bps: int = 0

    polymarket_gamma_url: str = "https://gamma-api.polymarket.com"
    polymarket_clob_url: str = "https://clob.polymarket.com"
    predictfun_api_url: str = "https://api.predict.fun"

    dedup_ttl_seconds: int = 600

    log_level: str = "INFO"
    log_json: bool = False


def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
