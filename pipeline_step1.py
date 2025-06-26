"""
Hamilton用データ変換関数群（ステップ1）
生データを読み込み、基本的なクリーニングと集計を行う
"""

import pandas as pd
from typing import Dict, Any
from column_lineage_tracker import ColumnLineageTracker

# カラムリネージトラッカーのインスタンス
tracker = ColumnLineageTracker()


@tracker.track_function
def raw_sales_data() -> pd.DataFrame:
    """
    生の売上データを読み込む

    Returns:
        pd.DataFrame: 生の売上データ
    """
    return pd.read_csv("data/sales_raw.csv")


@tracker.track_function
def cleaned_sales_data(raw_sales_data: pd.DataFrame) -> pd.DataFrame:
    """
    売上データのクリーニングを行う

    Args:
        raw_sales_data: 生の売上データ

    Returns:
        pd.DataFrame: クリーニング済み売上データ
    """
    # 欠損値を除去
    cleaned = raw_sales_data.dropna()

    # 売上金額が0以下のレコードを除去
    cleaned = cleaned[cleaned["amount"] > 0]

    # 日付列をdatetime型に変換
    cleaned["date"] = pd.to_datetime(cleaned["date"])

    return cleaned


@tracker.track_function
def daily_sales_summary(cleaned_sales_data: pd.DataFrame) -> pd.DataFrame:
    """
    日別売上サマリーを作成する

    Args:
        cleaned_sales_data: クリーニング済み売上データ

    Returns:
        pd.DataFrame: 日別売上サマリー
    """
    summary = (
        cleaned_sales_data.groupby("date")
        .agg({"amount": ["sum", "mean", "count"], "product_id": "nunique"})
        .round(2)
    )

    # カラム名をフラット化
    summary.columns = [
        "total_amount",
        "avg_amount",
        "transaction_count",
        "unique_products",
    ]

    return summary.reset_index()


def data_quality_metrics(
    raw_sales_data: pd.DataFrame, cleaned_sales_data: pd.DataFrame
) -> Dict[str, Any]:
    """
    データ品質メトリクスを計算する

    Args:
        raw_sales_data: 生の売上データ
        cleaned_sales_data: クリーニング済み売上データ

    Returns:
        Dict[str, Any]: データ品質メトリクス
    """
    return {
        "raw_record_count": len(raw_sales_data),
        "cleaned_record_count": len(cleaned_sales_data),
        "data_loss_rate": (len(raw_sales_data) - len(cleaned_sales_data))
        / len(raw_sales_data)
        * 100,
        "null_count": raw_sales_data.isnull().sum().sum(),
        "duplicate_count": raw_sales_data.duplicated().sum(),
    }
