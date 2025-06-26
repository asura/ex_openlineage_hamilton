"""
Hamilton用データ変換関数群（ステップ2）
ステップ1で作成した日別サマリーをさらに加工し、トレンド分析を行う
"""

import pandas as pd
from typing import Dict, Any, List


def enriched_daily_summary(daily_sales_summary: pd.DataFrame) -> pd.DataFrame:
    """
    日別サマリーにトレンド情報を追加する

    Args:
        daily_sales_summary: 日別売上サマリー

    Returns:
        pd.DataFrame: 拡張された日別サマリー
    """
    df = daily_sales_summary.copy()
    df = df.sort_values("date")

    # 移動平均（7日間）を計算
    df["total_amount_ma7"] = (
        df["total_amount"].rolling(window=7, min_periods=1).mean().round(2)
    )
    df["avg_amount_ma7"] = (
        df["avg_amount"].rolling(window=7, min_periods=1).mean().round(2)
    )

    # 前日比を計算
    df["total_amount_change"] = df["total_amount"].pct_change().round(4)
    df["avg_amount_change"] = df["avg_amount"].pct_change().round(4)

    # 曜日情報を追加
    df["day_of_week"] = pd.to_datetime(df["date"]).dt.day_name()
    df["is_weekend"] = pd.to_datetime(df["date"]).dt.dayofweek.isin([5, 6])

    return df


def weekly_aggregation(enriched_daily_summary: pd.DataFrame) -> pd.DataFrame:
    """
    週別集計を作成する

    Args:
        enriched_daily_summary: 拡張された日別サマリー

    Returns:
        pd.DataFrame: 週別集計データ
    """
    df = enriched_daily_summary.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["week"] = df["date"].dt.to_period("W")

    weekly = (
        df.groupby("week")
        .agg(
            {
                "total_amount": ["sum", "mean", "std"],
                "transaction_count": "sum",
                "unique_products": "mean",
                "is_weekend": "sum",  # 週末日数
            }
        )
        .round(2)
    )

    # カラム名をフラット化
    weekly.columns = [
        "weekly_total",
        "daily_avg",
        "daily_std",
        "weekly_transactions",
        "avg_unique_products",
        "weekend_days",
    ]

    return weekly.reset_index()


def trend_analysis(enriched_daily_summary: pd.DataFrame) -> Dict[str, Any]:
    """
    トレンド分析結果を計算する

    Args:
        enriched_daily_summary: 拡張された日別サマリー

    Returns:
        Dict[str, Any]: トレンド分析結果
    """
    df = enriched_daily_summary.copy()

    # 成長率の計算
    first_week_avg = df.head(7)["total_amount"].mean()
    last_week_avg = df.tail(7)["total_amount"].mean()
    growth_rate = (
        ((last_week_avg - first_week_avg) / first_week_avg * 100)
        if first_week_avg > 0
        else 0
    )

    # 最高・最低売上日
    max_sales_day = df.loc[df["total_amount"].idxmax()]
    min_sales_day = df.loc[df["total_amount"].idxmin()]

    # 平日vs週末の比較
    weekday_avg = df[~df["is_weekend"]]["total_amount"].mean()
    weekend_avg = df[df["is_weekend"]]["total_amount"].mean()

    return {
        "period_growth_rate": round(growth_rate, 2),
        "max_sales_amount": float(max_sales_day["total_amount"]),
        "max_sales_date": str(max_sales_day["date"]),
        "min_sales_amount": float(min_sales_day["total_amount"]),
        "min_sales_date": str(min_sales_day["date"]),
        "weekday_avg_sales": round(weekday_avg, 2),
        "weekend_avg_sales": round(weekend_avg, 2),
        "weekend_premium": round((weekend_avg - weekday_avg) / weekday_avg * 100, 2),
        "volatility": round(df["total_amount"].std(), 2),
    }


def top_performing_days(enriched_daily_summary: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    売上上位日のリストを作成する

    Args:
        enriched_daily_summary: 拡張された日別サマリー

    Returns:
        List[Dict[str, Any]]: 売上上位日のリスト
    """
    df = enriched_daily_summary.copy()
    top_days = df.nlargest(5, "total_amount")

    return [
        {
            "date": str(row["date"]),
            "total_amount": float(row["total_amount"]),
            "transaction_count": int(row["transaction_count"]),
            "day_of_week": row["day_of_week"],
            "is_weekend": bool(row["is_weekend"]),
        }
        for _, row in top_days.iterrows()
    ]
