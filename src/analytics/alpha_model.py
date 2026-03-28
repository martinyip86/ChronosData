import polars as pl
from src.analytics import indicators
from src.utils.weight_manager import WeightManager

class AlphaModel:
    def __init__(self,symbol:str):
        self.symbol = symbol
        self.config = WeightManager.load_weight(f"{self.symbol.replace('/','_').lower()}_weights.json")
        self.w_vamp = self.config['w_vamp']
        self.w_ofi = self.config['w_ofi']
        self.intercept = self.config['intercept']
        self.best_lag = self.config.get('best_lag',20)

    def generate_signal(self,df:pl.DataFrame):
        return df.with_columns([
            indicators.calc_vamp_expr(),
            indicators.calc_ofi_expr()
        ]).with_columns([
            ((pl.col('vamp') - pl.col('mid_price')) / pl.col('mid_price') * 10000).alias('vamp_bias_bp')
        ]).with_columns([
            (pl.col('vamp_bias_bp') * self.w_vamp + pl.col('factor_ofi_smooth') * self.w_ofi + self.intercept).alias('combined_alpha')
        ])