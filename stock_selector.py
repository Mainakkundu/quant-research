class FilterLogic:
    def _init_(self,config):
        self.config = config 
        self.universe_cfg = self.config.get('universe',{})
        self.strategy_cfg = self.config.get('strategy',{})
    
    def compute_stats(self,ohlcv):
        TRADING_DAYS_6M = int(self.universe_cfg.get('min_trading_days_6m',126))
        MAX_ZERO_VOL_RATIO = float(self.universe_cfg.get('max_zero_volume_ratio_6m',0.10))
        VOL_LOW = float(self.universe_cfg.get('vol_low',0.30))
        VOL_HIGH = float(self.universe_cfg)

        rows = []
        for ticker, df in ohlcv_dict.items():
            # Defensive checks
            if df is None or df.empty:
                rows.append({
                    'symbol': ticker,
                    'last_date': None,
                    'total_days': 0,
                    'trading_days_6m': 0,
                    'median_price': np.nan,
                    'median_volume': np.nan,
                    'avg_turnover_6m': 0.0,
                    'zero_vol_ratio_6m': 1.0,
                    'vol_1y': np.nan,
                    'pct_days_above_ma_long': np.nan
                })
                continue
            d = df.copy()
            d.index = pd.to_datetime(d.index).normalize()
            d = d.sort_index()

            total_days = len(d)
            last_date = d.index.max().date() if total_days > 0 else None

            tail_6m = d.iloc[-TRADING_DAYS_6M:] if total_days >= TRADING_DAYS_6M else d
            trading_days_6m = len(tail_6m)

            median_price = float(d['Close'].median()) if 'Close' in d.columns else np.nan
            median_volume = float(d['Volume'].median()) if 'Volume' in d.columns else np.nan

            if len(tail_6m) > 0 and ('Volume' in tail_6m.columns) and ('Close' in tail_6m.columns):
                turnover_series = (tail_6m['Volume'] * tail_6m['Close']).replace([np.inf, -np.inf], np.nan).dropna()
                avg_turnover_6m = float(turnover_series.mean()) if not turnover_series.empty else 0.0
                zero_vol_ratio_6m = float((tail_6m['Volume'] == 0).sum()) / max(1, len(tail_6m))
            else:
                avg_turnover_6m = 0.0
                zero_vol_ratio_6m = 1.0

            vol_1y = np.nan
            if 'Close' in d.columns:
                daily_ret = d['Close'].pct_change().dropna()
                if len(daily_ret) >= 30:
                    window = min(252, len(daily_ret))
                    vol_1y = float(daily_ret.iloc[-window:].std() * np.sqrt(252))
                else:
                    vol_1y = np.nan

            # pct days above long MA (if configured)
            pct_above = np.nan
            try:
                ma_long_days = int(self.strategy_cfg.get('ma_long', 60))
                if 'Close' in d.columns and len(d) >= ma_long_days + 1:
                    ma_long = d['Close'].rolling(window=ma_long_days, min_periods=ma_long_days).mean()
                    pct_above = float((d['Close'] > ma_long).sum() / max(1, (~ma_long.isna()).sum()))
            except Exception:
                pct_above = np.nan

            rows.append({
                'symbol': ticker,
                'last_date': last_date,
                'total_days': total_days,
                'trading_days_6m': trading_days_6m,
                'median_price': median_price,
                'median_volume': median_volume,
                'avg_turnover_6m': avg_turnover_6m,
                'zero_vol_ratio_6m': zero_vol_ratio_6m,
                'vol_1y': vol_1y,
                'pct_days_above_ma_long': pct_above
            })

        stats_df = pd.DataFrame(rows)
        stats_df['last_date'] = pd.to_datetime(stats_df['last_date'])
        return stats_df
    
    
    def apply_filters(self, stats_df: pd.DataFrame) -> pd.DataFrame:

        u = self.universe_cfg
        mask = (
            (stats_df['avg_turnover_6m'] >= u.get('min_avg_turnover_inr', 0)) &
            (stats_df['median_volume'] >= u.get('min_median_volume', 0)) &
            (stats_df['median_price'] >= u.get('min_price', 0)) &
            (stats_df['trading_days_6m'] >= u.get('min_trading_days_6m', 0)) &
            (stats_df['zero_vol_ratio_6m'] <= u.get('max_zero_volume_ratio_6m', 1.0)) &
            (stats_df['vol_1y'].notnull()) &
            (stats_df['vol_1y'] >= u.get('vol_low', 0.0)) &
            (stats_df['vol_1y'] <= u.get('vol_high', 999.0))
        )
        filtered = stats_df[mask].copy()
        filtered = filtered.sort_values('avg_turnover_6m', ascending=False)
        return filtered