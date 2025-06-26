# 再開・増分実行機能の比較分析

## 概要

Hamilton と OpenLineage の再開・増分実行機能について、従来の Makefile ベースのワークフローとの比較を含めて詳細に分析したドキュメントです。

## Hamilton フレームワークの機能

### 1. エラー停止からの再開機能

#### 強力なキャッシュ・チェックポイント機能
Hamilton は自動チェックポイント機能として動作する包括的なキャッシュシステムを提供します。

```python
# キャッシュ機能の有効化
driver = Driver(config, module).with_cache()

# エラー後の再実行でも、成功済みの処理は自動的にスキップ
result = driver.execute(['final_output'])
```

**主要な特徴：**
- 実行結果を自動保存し、同じ入力に対してはキャッシュから取得
- **カーネル再起動や計算機シャットダウン後も正確に作業を再開可能**
- 高コストな操作を保護（SQL結合、モデル訓練、大容量ダウンロード、有料LLM呼び出し）
- 中間結果の保持により、エラー発生箇所から正確に再開

### 2. 関数レベルの増分実行

#### 自動依存関係解決
Hamilton は関数のシグネチャから自動的にDAGを構築し、変更された部分のみを再実行します。

```python
# 例：ファイルが更新されても関数レベルで最適化

@cache
def load_data():  # ← この関数は変更されていない
    return pd.read_csv("data.csv")  # キャッシュヒット！

@cache  
def clean_data(raw_data):  # ← この関数のみ変更された
    # 新しいクリーニングロジックを追加
    return raw_data.dropna().reset_index()  # 再実行

@cache
def analyze_data(clean_data):  # ← この関数は変更されていないが
    # 入力（clean_data）が変わったので再実行される
    return clean_data.describe()
```

#### キャッシュキー生成の仕組み

**キャッシュキー = 関数のコードハッシュ + 入力データのバージョンハッシュ**

```python
# 疑似コード
cache_key = hash(function_code) + hash(input_data_versions)
```

**動作原理：**
1. 各関数のコードをハッシュ化してバージョン管理
2. 入力データも再帰的にハッシュ化（`List[Dict[str, float]]` なども対応）
3. コードとデータの両方が同じなら **キャッシュヒット**
4. どちらかが変更されていれば **再実行**

### 3. キャッシュ制御オプション

```python
@cache  # 各種オプション指定可能
def my_function(input_data):
    pass
```

**制御オプション：**
- **default**: 通常のキャッシュ有効
- **recompute**: 常に計算、キャッシュを使用しない
- **ignore**: データバージョンを下流のキャッシュキーから除外
- **disable**: キャッシュが無効化されているかのように動作

## OpenLineage の機能

### 1. エラー回復・影響分析

#### 根本原因分析支援
OpenLineage は標準化されたメタデータ収集により、パイプライン障害時の迅速な問題解決を支援します。

**主要機能：**
- パイプライン失敗時の根本原因の迅速特定
- 上流・下流への影響分析
- 依存関係の逆追跡による問題箇所の特定
- 変更前の影響範囲分析（impact analysis）

### 2. 増分データ処理対応

#### 増分モデルサポート
```python
# dbt 増分モデルなどの追跡例
# OpenLineage が自動的にメタデータを収集

{{ config(materialized='incremental') }}
select * from source_table
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

**対応範囲：**
- dbt増分モデルなどの増分処理パターンを追跡
- フル更新と増分更新の両方に対応
- 静的リネージ（設計時リネージ）も2023年7月から対応

### 3. 変更管理・リスク軽減

**変更前の安全性確認：**
- パイプライン変更前にimpact analysisを実行
- 重要なワークフローの破綻リスクを事前に把握
- チーム間での変更影響の共有

## Makefile との比較分析

### 機能比較表

| 比較項目 | Makefile | Hamilton | OpenLineage |
|---------|----------|----------|-------------|
| **粒度** | ファイルレベル | **関数レベル** | ジョブレベル |
| **判定基準** | ファイルタイムスタンプ | **関数コードハッシュ + 入力データバージョン** | メタデータ駆動 |
| **依存関係** | 手動指定必要 | **自動推論** | 実行時収集 |
| **部分変更時** | ファイル全体を再実行 | **変更された関数のみ再実行** | 影響範囲を可視化 |
| **エラー回復** | cleanビルドが必要 | **中間結果から再開** | 根本原因分析 |
| **効率性** | 低い（過度な再実行） | **高い（最小限の再実行）** | 高い（可視性向上） |

### Hamilton の優位性

1. **自動依存関係解決**: 手動でのMakefileルール記述が不要
2. **関数レベルの細かい制御**: ファイル単位でなく関数単位での依存関係
3. **賢いキャッシュ**: タイムスタンプでなくコンテンツとコードバージョンによる判定
4. **中間結果の保持**: エラー発生箇所から正確に再開可能
5. **コスト削減**: 高価な計算の重複実行を自動回避

### Makefile の限界

**従来の問題点：**
- ファイルタイムスタンプベースの粗い判定
- 手動での依存関係維持が必要
- 依存関係の欠落（MDs）や冗長性（RDs）が頻発
- エラー時は通常 clean ビルドが必要
- Pythonレベルの細かい制御ができない

## 実用的な移行戦略

### Phase 1: Hamilton導入
```python
# 既存のMakefileタスクをHamilton関数に変換
@cache
def extract_data():
    return pd.read_csv("raw_data.csv")

@cache
def transform_data(raw_data):
    return raw_data.dropna().reset_index()

@cache
def load_data(transformed_data):
    transformed_data.to_parquet("output.parquet")
    return "success"
```

### Phase 2: OpenLineage統合
```python
# リネージ追跡の追加
tracker = OpenLineageHamiltonTracker("my_pipeline")
driver = Driver(config, module).with_cache()

with tracker.track_execution():
    result = driver.execute(['load_data'])
```

### Phase 3: 監視・最適化
- キャッシュヒット率の監視
- 実行時間の分析
- ボトルネック関数の特定
- 並列実行の導入

## パフォーマンス最適化

### キャッシュ効率の最大化

```python
# 効率的なキャッシュ設計例
@cache
def expensive_model_training(features, params):
    # 高コストな処理はキャッシュ必須
    return train_model(features, params)

@cache(behavior="recompute")  
def daily_report(model, latest_data):
    # 毎日更新が必要な処理
    return generate_report(model, latest_data)
```

### 並列実行との組み合わせ

```python
# Parallelizable/Collectでの分散キャッシュ
@cache
@extract_columns("feature_columns")
def process_feature(feature_data: pd.Series) -> float:
    return expensive_feature_processing(feature_data)

@cache
def collect_features(process_feature: Collect[float]) -> pd.DataFrame:
    return pd.DataFrame(process_feature)
```

## 運用上の考慮事項

### 1. ストレージ管理
- キャッシュファイルサイズの監視
- 古いキャッシュの定期削除
- ディスク容量の管理

### 2. セキュリティ
- 機密データのキャッシュ制御
- キャッシュファイルのアクセス権限
- データの暗号化

### 3. デバッグとトラブルシューティング
```python
# ログレベルでキャッシュ動作を確認
import logging
logging.getLogger("hamilton").setLevel(logging.DEBUG)

# キャッシュ統計の確認
driver.execute(['final_result'], display_graph=True)
```

## まとめ

**Hamilton + OpenLineage の組み合わせは、従来の Makefile ベースのワークフローを完全に置き換え可能で、以下の点で大幅に改善されます：**

1. **自動化の向上**: 手動での依存関係管理が不要
2. **効率性の向上**: 関数レベルでの最適化により無駄な再実行を削減
3. **信頼性の向上**: 自動チェックポイントによるロバストなエラー回復
4. **可視性の向上**: OpenLineage による包括的なリネージ追跡
5. **開発体験の向上**: 高速な反復開発サイクル

現在のMakefileベースのワークフローは、機能的にも運用的にも大幅に改善された形で Hamilton + OpenLineage に移行できます。