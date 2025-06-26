# 開発ガイド

## 概要

このドキュメントでは、Hamilton + OpenLineageサンプルアプリケーションの開発に必要な技術情報を提供します。

## 型定義

### 使用されている主要な型

```python
from typing import Any, Dict, List, Union
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
```

### カスタム型定義

```python
# データ品質メトリクス型
DataQualityMetrics = Dict[str, Union[int, float]]

# トレンド分析結果型
TrendAnalysisResult = Dict[str, Union[int, float, str]]

# 売上上位日型
TopPerformingDay = Dict[str, Union[str, float, int, bool]]
TopPerformingDays = List[TopPerformingDay]

# パイプライン実行結果型
PipelineResults = Dict[str, Union[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]]]
```

## エラーハンドリング

### 一般的な例外

| 例外クラス | 発生条件 | 対処法 |
|-----------|---------|--------|
| `FileNotFoundError` | CSVファイルが見つからない | ファイルパスを確認 |
| `pd.errors.EmptyDataError` | CSVファイルが空 | データの存在を確認 |
| `pd.errors.ParserError` | CSV形式エラー | データ形式を確認 |
| `ValueError` | データ型変換エラー | 入力データの妥当性を確認 |
| `KeyError` | 必要なカラムが存在しない | データスキーマを確認 |
| `TypeError` | 型の不整合 | 関数の引数型を確認 |

### エラーログ設定

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)
```

### エラーハンドリング付き実行例

```python
import logging

try:
    results = run_pipeline_with_openlineage()
    logging.info("パイプライン実行成功")
except FileNotFoundError as e:
    logging.error(f"ファイルが見つかりません: {e}")
except pd.errors.EmptyDataError as e:
    logging.error(f"データが空です: {e}")
except Exception as e:
    logging.error(f"予期しないエラー: {e}")
    raise
```

## パフォーマンス考慮事項

### メモリ使用量目安

| データサイズ | メモリ使用量 | 推奨設定 |
|-------------|-------------|----------|
| 1,000 レコード | ~10MB | デフォルト |
| 10,000 レコード | ~50MB | チャンク処理検討 |
| 100,000 レコード | ~300MB | Dask使用推奨 |
| 1,000,000+ レコード | 1GB+ | 分散処理必須 |

### 実行時間目安

| 処理ステップ | 1,000 レコード | 10,000 レコード |
|-------------|---------------|----------------|
| データ読み込み | <0.1秒 | <0.5秒 |
| データクリーニング | <0.2秒 | <1秒 |
| 日別集計 | <0.1秒 | <0.5秒 |
| トレンド分析 | <0.1秒 | <0.3秒 |
| 全体実行 | <1秒 | <3秒 |

### パフォーマンス最適化のヒント

1. **大規模データの場合**:
   ```python
   # チャンク処理
   chunk_size = 10000
   for chunk in pd.read_csv("large_file.csv", chunksize=chunk_size):
       process_chunk(chunk)
   ```

2. **メモリ効率の改善**:
   ```python
   # データ型の最適化
   df = df.astype({
       'product_id': 'category',
       'customer_id': 'category',
       'amount': 'float32'
   })
   ```

3. **並列処理**:
   ```python
   # Daskを使用した分散処理
   import dask.dataframe as dd
   df = dd.read_csv("large_file.csv")
   ```

## 使用例集

### 基本的な使用パターン

```python
# 個別関数の実行
from hamilton import driver
import pipeline_step1, pipeline_step2

dr = driver.Driver({}, pipeline_step1, pipeline_step2)

# 特定の出力のみ取得
summary = dr.execute(["daily_sales_summary"])
print(summary["daily_sales_summary"].head())

# 複数出力の取得
results = dr.execute(["trend_analysis", "top_performing_days"])
print(results["trend_analysis"])
```

### カスタム設定での実行

```python
# 設定付きDriver
config = {
    "data_path": "/custom/path/data.csv",
    "output_format": "parquet"
}

dr = driver.Driver(config, pipeline_step1, pipeline_step2)
results = dr.execute(["weekly_aggregation"])
```

### デバッグ用の実行

```python
# Hamilton DAGの可視化
dr.visualize_execution(
    ["daily_sales_summary"], 
    "./dag_visualization.png"
)

# 実行プランの確認
execution_plan = dr.what_is_downstream_of("raw_sales_data")
print(execution_plan)
```

## テスト戦略

### 単体テストの例

```python
import pytest
import pandas as pd
from pipeline_step1 import cleaned_sales_data

class TestCleanedSalesData:
    def test_正常データの処理(self):
        # 正常なデータでテスト
        raw_data = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'product_id': ['P001', 'P002'],
            'amount': [100.0, 200.0],
            'customer_id': ['C001', 'C002']
        })
        
        result = cleaned_sales_data(raw_data)
        assert len(result) == 2
        assert result['date'].dtype == 'datetime64[ns]'
    
    def test_欠損値の除去(self):
        # 欠損値を含むデータでテスト
        raw_data = pd.DataFrame({
            'date': ['2024-01-01', None],
            'product_id': ['P001', 'P002'],
            'amount': [100.0, 200.0],
            'customer_id': ['C001', 'C002']
        })
        
        result = cleaned_sales_data(raw_data)
        assert len(result) == 1  # 欠損値を含む行が除去される
    
    def test_負の金額の除去(self):
        # 負の金額を含むデータでテスト
        raw_data = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-02'],
            'product_id': ['P001', 'P002'],
            'amount': [100.0, -50.0],
            'customer_id': ['C001', 'C002']
        })
        
        result = cleaned_sales_data(raw_data)
        assert len(result) == 1  # 負の金額を含む行が除去される
        assert all(result['amount'] > 0)
```

### 統合テストの例

```python
def test_パイプライン全体実行():
    """エンドツーエンドのパイプラインテスト"""
    from hamilton import driver
    import pipeline_step1, pipeline_step2
    
    # テスト用の小さなデータセットを準備
    setup_test_data()
    
    try:
        dr = driver.Driver({}, pipeline_step1, pipeline_step2)
        results = dr.execute([
            "daily_sales_summary",
            "trend_analysis",
            "data_quality_metrics"
        ])
        
        # 結果の検証
        assert "daily_sales_summary" in results
        assert "trend_analysis" in results
        assert "data_quality_metrics" in results
        
        # データ品質の検証
        metrics = results["data_quality_metrics"]
        assert metrics["data_loss_rate"] >= 0
        assert metrics["cleaned_record_count"] <= metrics["raw_record_count"]
        
    finally:
        cleanup_test_data()
```

## 開発環境セットアップ

### 必要なツール

```bash
# 開発用の追加パッケージ
pip install jupyter matplotlib seaborn

# コード品質チェック
pip install pre-commit bandit safety
```

### pre-commitの設定

`.pre-commit-config.yaml`:
```yaml
repos:
  - repo: https://github.com/psf/black
    rev: 25.1.0
    hooks:
      - id: black
  
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.12.0
    hooks:
      - id: ruff
      - id: ruff-format
  
  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.16.1
    hooks:
      - id: mypy
```

### Jupyter Notebook での開発

```python
# Jupyter での Hamilton DAG可視化
%load_ext hamilton.jupyter_extension

from hamilton import driver
import pipeline_step1, pipeline_step2

dr = driver.Driver({}, pipeline_step1, pipeline_step2)

# DAGの可視化
dr.visualize_execution(["daily_sales_summary"])

# インタラクティブな結果確認
results = dr.execute(["trend_analysis"])
print(results["trend_analysis"])
```

## カスタム関数の追加

### 新しいHamilton関数の作成

```python
# pipeline_step3.py
import pandas as pd
from typing import Dict, Any

def customer_segmentation(enriched_daily_summary: pd.DataFrame) -> Dict[str, Any]:
    """
    顧客セグメンテーション分析を実行する
    
    Args:
        enriched_daily_summary: 拡張された日別サマリー
        
    Returns:
        Dict[str, Any]: セグメンテーション結果
    """
    # 新しい分析ロジックを実装
    return {
        "high_value_days": 5,
        "low_value_days": 3,
        "avg_segment_value": 1500.0
    }

def seasonal_analysis(weekly_aggregation: pd.DataFrame) -> pd.DataFrame:
    """
    季節性分析を実行する
    
    Args:
        weekly_aggregation: 週別集計データ
        
    Returns:
        pd.DataFrame: 季節性分析結果
    """
    # 季節性分析のロジックを実装
    df = weekly_aggregation.copy()
    df['season_trend'] = df['weekly_total'].rolling(4).mean()
    return df
```

### 新しい関数の統合

```python
# main.py を更新
import pipeline_step1, pipeline_step2, pipeline_step3

dr = driver.Driver({}, pipeline_step1, pipeline_step2, pipeline_step3)

# 新しい出力を含めて実行
results = dr.execute([
    "customer_segmentation",
    "seasonal_analysis"
])
```

## デバッグのヒント

### Hamilton DAGのデバッグ

```python
# 依存関係の確認
dependencies = dr.what_is_downstream_of("raw_sales_data")
print("raw_sales_data に依存する関数:", dependencies)

# 実行プランの表示
execution_plan = dr.compute_plan(["daily_sales_summary"])
print("実行プラン:", execution_plan)

# 中間結果の確認
intermediate_results = dr.execute(["cleaned_sales_data"])
print(intermediate_results["cleaned_sales_data"].info())
```

### OpenLineageイベントのデバッグ

```python
# ローカルでのイベント確認
import json

class DebugOpenLineageClient:
    def emit(self, event):
        print("OpenLineage Event:")
        print(json.dumps(event.__dict__, indent=2, default=str))

# デバッグ用クライアントを使用
tracker.client = DebugOpenLineageClient()
```

## 拡張の方向性

### 1. データソースの追加

```python
def database_sales_data() -> pd.DataFrame:
    """データベースから売上データを読み込む"""
    import sqlalchemy
    engine = sqlalchemy.create_engine("postgresql://...")
    return pd.read_sql("SELECT * FROM sales", engine)

def api_sales_data() -> pd.DataFrame:
    """APIから売上データを取得する"""
    import requests
    response = requests.get("https://api.example.com/sales")
    return pd.DataFrame(response.json())
```

### 2. 出力形式の追加

```python
def save_to_parquet(daily_sales_summary: pd.DataFrame) -> str:
    """Parquet形式で保存する"""
    output_path = "output/daily_sales_summary.parquet"
    daily_sales_summary.to_parquet(output_path)
    return output_path

def save_to_database(weekly_aggregation: pd.DataFrame) -> bool:
    """データベースに保存する"""
    import sqlalchemy
    engine = sqlalchemy.create_engine("postgresql://...")
    weekly_aggregation.to_sql("weekly_summary", engine, if_exists="replace")
    return True
```

### 3. リアルタイム処理

```python
def streaming_processor():
    """ストリーミングデータ処理"""
    import kafka
    
    consumer = kafka.KafkaConsumer('sales-stream')
    for message in consumer:
        # リアルタイムでデータ処理
        process_realtime_data(message.value)
```

## 関連ドキュメント
- [データフロー設計](dataflow.md)
- [アーキテクチャ設計](architecture.md)
- [運用ガイド](operations.md)
- [ワークフローパターン](workflow_patterns.md)
- [README](../README.md)