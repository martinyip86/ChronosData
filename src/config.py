from pydantic_settings import BaseSettings,SettingsConfigDict

class Settings(BaseSettings):
    binance_api_key:str = ""
    binance_secret:str = ""

    env:str = "development"
    symbol:str = "BTC/USDT"

    base_data_path:str = 'app/data'
    base_log_path:str = 'app/logs'

    model_config = SettingsConfigDict(env_file=".env",extra='ignore')

config = Settings()