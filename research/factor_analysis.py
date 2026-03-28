import polars as pl
import numpy as np
from sklearn.linear_model import LinearRegression
from src.analytics import indicators

class AlphaResearch:
    def __init__(self,df:pl.DataFrame):
        self.df = df
        self.best_lag = 20
        self.weights = {"w_vamp":0.0,"w_ofi":0.0,"intercept":0.0}

    def compute_features(self,depth=5,window=20):
        self.df = self.df.with_columns([
            indicators.calc_vamp_expr(depth=depth),
            indicators.calc_ofi_expr(window=window)
        ]).with_columns([
            ((pl.col('vamp') - pl.col('mid_price')) / pl.col('mid_price') * 10000).alias('vamp_bias_bp')
        ])
        return self
    
    def label_data(self,lags=[5,10,20,50,100],split_ratio=0.7):
        results = {}

        for lag in lags:
            col_name = f"target_{lag}_tick"
            self.df = self.df.with_columns([
                ((pl.col('mid_price').shift(-lag) - pl.col('mid_price')) / pl.col('mid_price') * 10000).alias(col_name)
            ])

        n = len(self.df)
        split_idx = int(n * split_ratio)

        train_df = self.df.slice(0,split_idx)

        for lag in lags:
            col_name = f"target_{lag}_tick"
            train_df = train_df.with_columns([
                ((pl.col('mid_price').shift(-lag) - pl.col('mid_price')) / pl.col('mid_price') * 10000).alias(col_name)
            ])
            ic = abs(train_df.drop_nulls().select(pl.corr('vamp_bias_bp',col_name)).item())
            results[lag] = ic

        self.best_lag = max(results,key=results.get)
        print(f"检测到最佳预测窗口: {self.best_lag} ticks")
        return self

    def train_combined_signal(self,split_ratio=0.7):
        target = f"target_{self.best_lag}_tick"
        full_df = self.df.select(['vamp_bias_bp','factor_ofi_smooth',target]).drop_nulls()

        n = len(full_df)

        split_idx = int(n * split_ratio)

        train_df = full_df.slice(0,split_idx)
        valid_df = full_df.slice(split_idx,n - split_idx)

        X_train = train_df.select(['vamp_bias_bp','factor_ofi_smooth']).to_numpy()
        y_train = train_df.select([target]).to_numpy()

        model = LinearRegression()
        model.fit(X_train,y_train)

        X_valid = valid_df.select(['vamp_bias_bp','factor_ofi_smooth']).to_numpy()
        y_valid = valid_df.select([target]).to_numpy()

        y_pred = model.predict(X_valid)

        valid_df = valid_df.with_columns(pl.Series('pred',y_pred.flatten()))
        valid_ic = valid_df.select(pl.corr('pred',target)).item()

        train_ic = train_df.with_columns(pl.Series('pred',model.predict(X_train).flatten())).select(pl.corr('pred',target)).item()

        print(f"train_ic: {train_ic:.4f}")
        print(f"valid_ic: {valid_ic:.4f}")
        print(f"权重衰减率: {(train_ic - valid_ic)/train_ic:.2%}")

        if valid_ic > 0.02:
            X_full = full_df.select(['vamp_bias_bp','factor_ofi_smooth']).to_numpy()
            y_full = full_df.select([target]).to_numpy()

            model.fit(X_full,y_full)

            coef = model.coef_.flatten()
            self.weights['w_vamp'],self.weights['w_ofi'] = coef
            self.weights['intercept'] = model.intercept_[0]

            self.df = self.df.with_columns([
                (pl.col('vamp_bias_bp') * self.weights['w_vamp'] + pl.col('factor_ofi_smooth') * self.weights['w_ofi'] + self.weights['intercept']).alias('combined_alpha')
            ])
            final_ic = self.df.drop_nulls().select(pl.corr('combined_alpha',target)).item()

            print(f"回归完成! 权重: {self.weights}")
            print(f"⚠️ final IC (in-sample, biased): {final_ic:.4f}")
        return self