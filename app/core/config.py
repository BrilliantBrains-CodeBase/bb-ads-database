from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── App ──────────────────────────────────────────────────────
    app_env: Literal["development", "staging", "production"] = "development"
    app_debug: bool = False
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_workers: int = 2
    app_instance: str = "api"  # api1 | api2 | worker

    # ── MongoDB ──────────────────────────────────────────────────
    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db_name: str = "bb_ads"
    mongodb_max_pool_size: int = 50

    # ── Redis ────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"

    # ── JWT (RS256) ───────────────────────────────────────────────
    jwt_private_key_path: str = "./keys/private.pem"
    jwt_public_key_path: str = "./keys/public.pem"
    jwt_access_token_expire_minutes: int = 15
    jwt_refresh_token_expire_days: int = 7

    # ── Encryption ────────────────────────────────────────────────
    # 32-byte hex = 64 chars. Dev default is a zeroed key — NEVER use in prod.
    # Set a real value in .env / Doppler before deploying.
    encryption_key: str = Field(
        default="0" * 64,
        min_length=64,
    )

    # ── CORS ──────────────────────────────────────────────────────
    cors_allowed_origins: str | list[str] = ["http://localhost:3000", "http://localhost:5173"]

    @field_validator("cors_allowed_origins", mode="before")
    @classmethod
    def parse_origins(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [o.strip() for o in v.split(",") if o.strip()]
        return v

    # ── Rate Limiting ─────────────────────────────────────────────
    auth_rate_limit_attempts: int = 5
    auth_rate_limit_window_seconds: int = 60

    # ── Brand Storage ─────────────────────────────────────────────
    brand_storage_root: str = "/data/brands"

    # ── Google Ads ────────────────────────────────────────────────
    google_ads_developer_token: str = ""
    google_ads_client_id: str = ""
    google_ads_client_secret: str = ""

    # ── Meta Ads ──────────────────────────────────────────────────
    meta_app_id: str = ""
    meta_app_secret: str = ""

    # ── Interakt ──────────────────────────────────────────────────
    interakt_base_url: str = "https://api.interakt.ai"

    # ── Anthropic ─────────────────────────────────────────────────
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-opus-4-6"
    anthropic_max_tokens: int = 4096

    # ── SendGrid ──────────────────────────────────────────────────
    sendgrid_api_key: str = ""
    sendgrid_from_email: str = "reports@youragency.com"
    sendgrid_from_name: str = "BB Ads Analytics"

    # ── ClickUp ───────────────────────────────────────────────────
    clickup_api_token: str = ""
    clickup_onboarding_list_id: str = ""

    # ── Telegram ─────────────────────────────────────────────────
    telegram_bot_token: str = ""
    telegram_alert_chat_id: str = ""

    # ── Backup ────────────────────────────────────────────────────
    b2_key_id: str = ""
    b2_application_key: str = ""
    b2_bucket_name: str = "bb-ads-backups"

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def is_development(self) -> bool:
        return self.app_env == "development"


@lru_cache
def get_settings() -> Settings:
    return Settings()
