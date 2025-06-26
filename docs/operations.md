# 運用ガイド

## 概要

このドキュメントでは、Hamilton + OpenLineageサンプルアプリケーションの運用、監視、パフォーマンス最適化について説明します。

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

### 計算効率

#### ベストプラクティス
- **グループ化**: pandas.groupbyを最大限活用
- **ベクトル化**: ループを避けてベクトル演算を使用
- **インデックス**: 適切なインデックス設定

#### 大規模データの処理
```python
# チャンク処理の例
chunk_size = 10000
for chunk in pd.read_csv("large_file.csv", chunksize=chunk_size):
    process_chunk(chunk)

# メモリ効率の改善
df = df.astype({
    'product_id': 'category',
    'customer_id': 'category', 
    'amount': 'float32'
})

# Daskを使用した分散処理
import dask.dataframe as dd
df = dd.read_csv("large_file.csv")
```

## 監視・ログ

### ログレベル設定

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
```

### メトリクス監視

#### システムメトリクス
- **CPU使用率**: プロセス全体の負荷
- **メモリ使用量**: ピーク時とベースライン
- **ディスクI/O**: データ読み書きの速度
- **実行時間**: 各ステップとパイプライン全体

#### アプリケーションメトリクス
- **データ品質**: レコード数、エラー率
- **処理量**: 1秒当たりのレコード数
- **成功率**: 正常終了の割合

### アラート設定

```python
def check_data_quality(metrics):
    """データ品質アラートのチェック"""
    if metrics['data_loss_rate'] > 10:  # 10%以上のデータロス
        logger.warning(f"High data loss rate: {metrics['data_loss_rate']:.2f}%")
        
    if metrics['null_count'] > 100:  # 100個以上の欠損値
        logger.warning(f"High null count: {metrics['null_count']}")
```

## エラーハンドリング

### 一般的な例外と対処法

| 例外クラス | 発生条件 | 対処法 | 重要度 |
|-----------|---------|-------|--------|
| `FileNotFoundError` | CSVファイルが見つからない | ファイルパスを確認 | 高 |
| `pd.errors.EmptyDataError` | CSVファイルが空 | データの存在を確認 | 高 |
| `pd.errors.ParserError` | CSV形式エラー | データ形式を確認 | 中 |
| `ValueError` | データ型変換エラー | 入力データの妥当性を確認 | 中 |
| `MemoryError` | メモリ不足 | チャンク処理に変更 | 高 |

### 復旧手順

#### データファイルの問題
1. **バックアップからの復旧**: 正常なデータファイルを復元
2. **部分データでの実行**: 利用可能なデータで分析を継続
3. **手動修正**: 明らかなデータエラーの修正

#### システムリソースの問題
1. **メモリ不足**: チャンク処理への切り替え
2. **ディスク容量不足**: 古いログ・出力ファイルの削除
3. **CPU高負荷**: 処理の分散化

## スケーラビリティ

### 水平スケーリング

#### マルチプロセシング
```python
from multiprocessing import Pool
import pandas as pd

def process_chunk(chunk_file):
    """チャンクファイルを個別に処理"""
    return process_data(pd.read_csv(chunk_file))

# 並列処理の実行
with Pool() as pool:
    results = pool.map(process_chunk, chunk_files)
```

#### 分散処理（Dask）
```python
import dask.dataframe as dd
from dask.distributed import Client

# Daskクライアントの初期化
client = Client('scheduler-address:8786')

# 分散データフレームでの処理
df = dd.read_csv('large_dataset.csv')
result = df.groupby('date').amount.sum().compute()
```

### 垂直スケーリング

#### メモリ最適化
- **データ型の最適化**: category型、float32の活用
- **メモリマップファイル**: 大容量ファイルの効率的読み込み
- **ガベージコレクション**: 適切なメモリ管理

#### CPU最適化
- **NumPy操作**: pandas操作をNumPyで最適化
- **並列計算**: numba、Cythonの活用
- **マルチスレッド**: I/Oバウンドタスクの並列化

## バックアップ・復旧

### データバックアップ戦略

#### 自動バックアップ
```bash
#!/bin/bash
# daily_backup.sh
DATE=$(date +%Y%m%d)
BACKUP_DIR="/backup/$DATE"

mkdir -p $BACKUP_DIR
cp -r data/ $BACKUP_DIR/
cp -r output/ $BACKUP_DIR/
tar -czf $BACKUP_DIR.tar.gz $BACKUP_DIR
rm -rf $BACKUP_DIR
```

#### 世代管理
- **日次バックアップ**: 過去7日分
- **週次バックアップ**: 過去4週分
- **月次バックアップ**: 過去12ヶ月分

### 災害復旧手順

1. **状況確認**: エラーログとシステム状態の確認
2. **データ整合性チェック**: 既存データの検証
3. **バックアップからの復元**: 最新の正常なデータを復元
4. **パイプライン再実行**: 失敗したジョブの再実行
5. **結果検証**: 出力データの妥当性確認

## セキュリティ

### データプライバシー

#### 個人情報の保護
```python
def anonymize_customer_data(df):
    """顧客データの匿名化"""
    df['customer_id'] = df['customer_id'].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()[:8]
    )
    return df
```

#### アクセス制御
- **ファイルパーミッション**: 適切な読み書き権限設定
- **ディレクトリアクセス**: 必要最小限のアクセス権
- **ログファイル**: 機密情報の除外

### 監査ログ

```python
def audit_log(operation, user, details):
    """監査ログの記録"""
    timestamp = datetime.now().isoformat()
    audit_entry = {
        'timestamp': timestamp,
        'operation': operation,
        'user': user,
        'details': details
    }
    
    with open('audit.log', 'a') as f:
        f.write(json.dumps(audit_entry) + '\n')
```

## 拡張性

### 新機能の追加

#### 新しいデータソース
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

#### 新しい出力形式
```python
def save_to_parquet(daily_sales_summary: pd.DataFrame) -> str:
    """Parquet形式で保存する"""
    output_path = "output/daily_sales_summary.parquet"
    daily_sales_summary.to_parquet(output_path)
    return output_path
```

### アーキテクチャの発展

#### マイクロサービス化
- **データ取得サービス**: 複数データソースの統合
- **処理サービス**: Hamiltonエンジンの独立化
- **出力サービス**: 結果配信の専門化

#### クラウド対応
- **コンテナ化**: Docker、Kubernetesでの実行
- **サーバーレス**: AWS Lambda、Cloud Functionsでの実行
- **マネージドサービス**: クラウドのデータ処理サービス活用

## トラブルシューティング

### よくある問題と解決法

#### パフォーマンス問題
**症状**: 実行時間が長い
**原因**: 大量データ、非効率な処理
**解決法**: 
- チャンク処理の導入
- インデックスの最適化
- 並列処理の活用

#### メモリ不足
**症状**: `MemoryError`が発生
**原因**: 大きなデータセットの一括読み込み
**解決法**:
- チャンクサイズの調整
- データ型の最適化
- 不要なデータの早期削除

#### データ品質問題
**症状**: 異常な分析結果
**原因**: 不正なデータ、処理ロジックのバグ
**解決法**:
- データ検証ルールの強化
- 単体テストの追加
- 段階的なデバッグ実行

### デバッグ手順

1. **ログ確認**: エラーメッセージとスタックトレース
2. **データ検証**: 入力データの妥当性確認
3. **段階実行**: 各ステップの個別実行
4. **中間結果確認**: 処理途中のデータ状態チェック
5. **設定確認**: パラメータと環境変数の検証

## 関連ドキュメント
- [アーキテクチャ設計](architecture.md)
- [データフロー設計](dataflow.md)
- [開発ガイド](development_guide.md)
- [README](../README.md)