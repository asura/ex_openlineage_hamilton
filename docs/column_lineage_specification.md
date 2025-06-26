# カラムレベルリネージ追跡仕様書

## 概要

`column_lineage_tracker.py`は、Hamilton関数におけるカラムレベルのデータリネージを自動追跡するツールです。従来のOpenLineageによるジョブレベル追跡では捉えられない、個々のカラムの変換詳細を記録・分析します。

## 機能仕様

### 1. 基本機能

#### 1.1 自動リネージ追跡
- **目的**: Hamilton関数の実行時に入出力カラムの関係を自動記録
- **動作**: デコレータパターンで関数をラップし、実行前後でDataFrame分析
- **対象**: pandas.DataFrameの入出力を持つ関数

#### 1.2 カラム変換推論
- **完全一致**: `amount` → `amount` (コピー操作)
- **部分一致**: `amount` → `total_amount` (派生操作)
- **コード解析**: ソースコード内でのカラム言及による関係推論

#### 1.3 操作タイプ分類
- `created`: 新規作成（例: CSVからの読み込み）
- `copied`: 直接コピー（例: 同名カラムの保持）
- `transformed`: 何らかの変換（例: 型変換、計算）
- `aggregated`: 集計処理（例: sum, mean, count）
- `derived`: 派生計算（例: 移動平均、変化率）
- `windowed`: ウィンドウ関数（例: rolling）

## API仕様

### 2. ColumnLineageTracker クラス

#### 2.1 コンストラクタ
```python
def __init__(self, lineage_dir: str = "column_lineage")
```

**パラメータ**:
- `lineage_dir`: リネージファイルの保存ディレクトリ

**初期化処理**:
- 保存ディレクトリの作成
- 内部状態の初期化

#### 2.2 track_function デコレータ
```python
def track_function(self, func: Callable) -> Callable
```

**目的**: Hamilton関数にリネージ追跡機能を追加

**使用例**:
```python
tracker = ColumnLineageTracker()

@tracker.track_function
def clean_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data.dropna()
```

**処理フロー**:
1. 関数実行前の入力分析
2. 関数実行
3. 関数実行後の出力分析
4. カラム変換の推論
5. リネージ情報のファイル保存

#### 2.3 内部メソッド仕様

##### _analyze_inputs()
```python
def _analyze_inputs(self, func: Callable, args: tuple, kwargs: dict) -> Dict[str, Any]
```

**機能**: 関数の入力引数を詳細分析

**分析内容**:
- **DataFrameの場合**:
  - カラム名リスト
  - データ型情報
  - データ形状（行数×列数）
  - サンプル値（各カラム最大3個）
- **その他パラメータ**:
  - 型情報
  - 値の文字列表現（100文字まで）

**戻り値構造**:
```json
{
  "datasets": {
    "parameter_name": {
      "columns": ["col1", "col2"],
      "dtypes": {"col1": "int64", "col2": "object"},
      "shape": [100, 4],
      "sample_values": {"col1": ["1", "2", "3"], "col2": ["a", "b", "c"]}
    }
  },
  "parameters": {
    "other_param": {
      "type": "str",
      "value": "parameter_value"
    }
  }
}
```

##### _analyze_output()
```python
def _analyze_output(self, func_name: str, result: Any) -> Dict[str, Any]
```

**機能**: 関数の出力結果を詳細分析

**対応型**:
- `pd.DataFrame`: カラム情報の詳細分析
- `dict`: キー情報とサンプル値
- `list`: 長さとサンプル要素
- その他: 型情報

##### _infer_column_transformations()
```python
def _infer_column_transformations(
    self, func_name: str, 
    input_analysis: Dict, 
    output_analysis: Dict, 
    func_source: str
) -> List[Dict[str, Any]]
```

**機能**: 入出力分析からカラム変換を推論

**推論ロジック**:
1. **直接マッピング**: 同名カラムの対応
2. **部分マッピング**: 名前に包含関係があるカラム
3. **コード解析**: ソースコード内での言及パターン
4. **新規作成**: 対応する入力カラムがない出力カラム

**変換レコード構造**:
```json
{
  "source_dataset": "input_df_name",
  "source_column": "source_col",
  "target_dataset": "output_df_name", 
  "target_column": "target_col",
  "transformation_logic": "extracted_code_lines",
  "function_name": "function_name",
  "operation": "copied|transformed|aggregated|derived",
  "confidence": 0.95
}
```

##### _extract_transformation_logic()
```python
def _extract_transformation_logic(self, input_col: str, output_col: str, func_source: str) -> str
```

**機能**: ソースコードから変換ロジックを抽出

**抽出方法**:
- 入力・出力カラム名が言及されている行を特定
- 関連する処理行を最大3行まで抽出
- パイプ文字（|）で連結して返却

### 3. ColumnLineageViewer クラス

#### 3.1 コンストラクタ
```python
def __init__(self, lineage_dir: str = "column_lineage")
```

#### 3.2 表示メソッド

##### show_column_lineage_table()
```python
def show_column_lineage_table(self) -> None
```

**機能**: CRUD表形式でカラムリネージを表示

**表示形式**:
```
Function             Operation  Source                  Target                 Transformation
cleaned_sales_data   copied     raw_data.amount        cleaned_data.amount    data.dropna()
daily_summary        aggregated cleaned_data.amount    summary.total_amount   .groupby().sum()
```

##### show_column_journey()
```python
def show_column_journey(self, column_pattern: str) -> None
```

**機能**: 特定カラムの変換履歴を時系列表示

**パラメータ**:
- `column_pattern`: 追跡したいカラム名（部分一致）

**表示内容**:
- ステップ番号
- 関数名
- 変換元 → 変換先
- 操作タイプ
- 変換ロジック
- 実行時刻

##### show_function_impact()
```python
def show_function_impact(self, function_name: str) -> None
```

**機能**: 特定関数の影響範囲を分析表示

**表示内容**:
- 関数の実行履歴
- 入力カラム一覧
- 出力カラム一覧
- 詳細な変換マッピング

## データ構造仕様

### 4. ColumnLineageRecord
```python
@dataclass
class ColumnLineageRecord:
    timestamp: str                              # 実行時刻（ISO形式）
    function_name: str                          # 関数名
    input_columns: Dict[str, List[str]]         # 入力カラム {dataset: [columns]}
    output_columns: Dict[str, List[str]]        # 出力カラム {dataset: [columns]}
    column_transformations: List[Dict[str, Any]] # 変換詳細リスト
    operation_type: str                         # CREATE|READ|UPDATE|DELETE
    data_types: Dict[str, str]                  # データ型情報 {dataset.column: type}
```

### 5. ColumnTransformation
```python
@dataclass  
class ColumnTransformation:
    source_dataset: str     # 変換元データセット名
    source_column: str      # 変換元カラム名
    target_dataset: str     # 変換先データセット名
    target_column: str      # 変換先カラム名
    transformation_logic: str # 変換ロジック（コード抜粋）
    function_name: str      # 関数名
    operation: str          # 操作タイプ
```

## 保存ファイル仕様

### 6. ファイル命名規則
```
{function_name}_{timestamp}.json
```

**例**: `cleaned_sales_data_2025-06-27T07-52-15.577303.json`

### 7. ファイル構造
```json
{
  "lineage_summary": {
    "timestamp": "2025-06-27T07:52:15.577303",
    "function_name": "cleaned_sales_data", 
    "input_columns": {"raw_sales_data": ["date", "amount"]},
    "output_columns": {"cleaned_sales_data": ["date", "amount"]},
    "column_transformations": [...],
    "operation_type": "UPDATE",
    "data_types": {
      "raw_sales_data.date": "object",
      "cleaned_sales_data.date": "datetime64[ns]"
    }
  },
  "input_analysis": {
    "datasets": {...},
    "parameters": {...}
  },
  "output_analysis": {
    "type": "DataFrame",
    "datasets": {...}
  },
  "function_source": "def cleaned_sales_data(...):\n    ..."
}
```

## CLI仕様

### 8. コマンドライン引数

#### 基本形式
```bash
python column_lineage_tracker.py [OPTIONS]
```

#### オプション一覧

| オプション | 短縮形 | 必須 | 説明 | 例 |
|-----------|--------|-----|------|-----|
| `--command` | `-c` | ○ | 表示モード選択 | `table` `journey` `function` |
| `--column` | `-col` | △ | カラム名パターン | `amount` |
| `--function` | `-f` | △ | 関数名 | `cleaned_sales_data` |

#### 使用例
```bash
# CRUD表表示
python column_lineage_tracker.py -c table

# 特定カラムの履歴追跡  
python column_lineage_tracker.py -c journey -col amount

# 関数影響分析
python column_lineage_tracker.py -c function -f daily_sales_summary
```

## 設定・カスタマイズ

### 9. 設定可能項目

#### 9.1 推論精度調整
```python
def _calculate_confidence(self, input_col: str, output_col: str, func_source: str) -> float:
    confidence = 0.5  # ベース信頼度
    
    if input_col == output_col:
        confidence += 0.4  # 完全一致ボーナス
    elif input_col in output_col:
        confidence += 0.3  # 部分一致ボーナス
    
    if input_col in func_source and output_col in func_source:
        confidence += 0.2  # コード言及ボーナス
    
    return min(confidence, 1.0)
```

#### 9.2 サンプル値数調整
```python
def _get_sample_values(self, df: pd.DataFrame, n_samples: int = 3) -> Dict[str, List[Any]]:
    # n_samples で取得する値の数を調整可能
```

#### 9.3 変換ロジック抽出行数
```python
def _extract_transformation_logic(self, input_col: str, output_col: str, func_source: str) -> str:
    # relevant_lines.append(line.strip())
    return ' | '.join(relevant_lines[:3])  # 最大3行まで
```

## 制限事項・注意点

### 10. 技術的制限

#### 10.1 対応データ型
- **完全対応**: `pandas.DataFrame`
- **部分対応**: `dict`, `list`
- **非対応**: numpy配列、カスタムオブジェクト

#### 10.2 推論精度の限界
- **高精度**: 同名カラム、部分一致カラム
- **中精度**: コード解析による推論
- **低精度**: 複雑な変換ロジック、動的カラム名

#### 10.3 パフォーマンス考慮
- **メモリ**: 大規模DataFrameでのサンプル値取得
- **ディスク**: 大量の関数実行でのファイル生成
- **CPU**: ソースコード解析処理

### 11. 運用上の注意

#### 11.1 ファイル管理
- 定期的な古いファイルの削除推奨
- ディスク容量の監視
- バックアップ戦略の検討

#### 11.2 セキュリティ
- サンプル値に機密情報が含まれる可能性
- ソースコードがファイルに保存される点
- 適切なアクセス権限設定

## 拡張・改善案

### 12. 将来的な機能拡張

#### 12.1 推論精度向上
- 機械学習による変換パターン学習
- より高度なコード解析（AST解析）
- データ型変化パターンの活用

#### 12.2 可視化機能
- Webベースのリネージ可視化
- グラフィカルなデータフロー表示
- インタラクティブな履歴探索

#### 12.3 統合機能
- OpenLineageとの統合
- Hamilton可視化機能との連携
- 外部データカタログとの連携

#### 12.4 パフォーマンス最適化
- 非同期でのファイル保存
- データベースによる履歴管理
- インクリメンタル解析

## 関連ドキュメント
- [アーキテクチャ設計](architecture.md)
- [データフロー設計](dataflow.md)
- [ワークフローパターン](workflow_patterns.md)
- [README](../README.md)