from __future__ import annotations
import yaml
from pathlib import Path
from pydantic import BaseModel

class AppConfig(BaseModel):
    name: str
    environment: str
    approvals: dict

class AuthConfig(BaseModel):
    demo_superadmin_user: str
    demo_superadmin_pass: str

class BrandingDefaults(BaseModel):
    default_degree_branding: bool = True

class DBConfig(BaseModel):
    url: str

class Settings(BaseModel):
    app: AppConfig
    auth: AuthConfig
    branding: BrandingDefaults
    db: DBConfig

def load_settings(path: str | Path = Path(__file__).resolve().parents[1] / "config" / "settings.yaml") -> Settings:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return Settings(
        app=AppConfig(**data["app"]),
        auth=AuthConfig(**data["auth"]),
        branding=BrandingDefaults(**data["branding"]),
        db=DBConfig(**data["db"]),
    )
