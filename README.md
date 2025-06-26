# Hamilton + OpenLineage データパイプライン統合サンプル

このプロジェクトは、[Hamilton](https://github.com/dagworks-inc/hamilton)と[OpenLineage](https://openlineage.io/)を統合したデータパイプラインのサンプル実装です。

## 目的

- **Hamilton**: 宣言的なデータフロー記述によるDAG構築
- **OpenLineage**: データリネージ（系譜）の自動追跡と可視化
- **統合効果**: 型安全なデータ変換とリネージ追跡の組み合わせ

## 特徴

### Hamiltonの利点
- 🔄 **宣言的なデータフロー**: 関数定義から自動でDAG構築
- 🛡️ **型安全性**: 関数の入出力型から依存関係を自動推論
- 🧪 **テスト容易性**: 各ステップが純粋関数なので単体テストが簡単
- 📊 **可視化**: DAGの自動生成・可視化

### OpenLineageの利点  
- 📈 **データリネージ追跡**: データの変換履歴を自動記録
- 🔍 **デバッグ支援**: データフローの実行履歴とメタデータを保持
- 📋 **監査性**: データの変更履歴を追跡可能
- 🔗 **相互運用性**: 様々なデータツールとの統合

## アーキテクチャ

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   生データ      │    │   Hamilton DAG   │    │  OpenLineage    │
│  (sales_raw.csv) │───▶│  データ変換      │───▶│  リネージ追跡   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   分析結果出力   │
                       │  (CSV/JSON)      │
                       └──────────────────┘
```

## データフロー

### ステップ1: 基本的なデータ処理
```
raw_sales_data → cleaned_sales_data → daily_sales_summary
                                   ↘
                                    data_quality_metrics
```

### ステップ2: 高度な分析
```
daily_sales_summary → enriched_daily_summary → weekly_aggregation
                                            ↘
                                             trend_analysis
                                            ↘
                                             top_performing_days
```

## セットアップ

### 必要要件
- Python 3.10+
- 仮想環境の作成を推奨

### インストール
```bash
# リポジトリをクローン
git clone <repository-url>
cd ex_openlineage_hamilton

# 仮想環境を作成・有効化
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\\Scripts\\activate  # Windows

# 依存関係をインストール
pip install -r requirements.txt
```

### セットアップスクリプトの実行
```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

## 使用法

### 基本的な実行
```bash
python main.py
```

### 実行結果
実行すると以下のファイルが`output/`ディレクトリに生成されます：

**CSV ファイル（DataFrame）:**
- `daily_sales_summary.csv`: 日別売上サマリー
- `enriched_daily_summary.csv`: トレンド情報付き日別サマリー  
- `weekly_aggregation.csv`: 週別集計データ

**JSON ファイル（分析結果）:**
- `trend_analysis.json`: トレンド分析結果
- `top_performing_days.json`: 売上上位日リスト
- `data_quality_metrics.json`: データ品質メトリクス

### OpenLineageイベント
パイプライン実行時に以下のイベントが生成されます：
- `START`: パイプライン開始
- `COMPLETE`: パイプライン完了
- `FAIL`: パイプライン失敗（エラー時）

## ファイル構成

```
.
├── README.md                  # このファイル
├── requirements.txt           # Python依存関係
├── main.py                   # メイン実行スクリプト
├── pipeline_step1.py         # データ処理ステップ1
├── pipeline_step2.py         # データ処理ステップ2
├── data/
│   └── sales_raw.csv         # サンプルデータ
├── output/                   # 実行結果出力ディレクトリ
├── scripts/
│   └── setup.sh             # セットアップスクリプト
└── docs/                    # 設計文書
    ├── architecture.md      # アーキテクチャ設計
    ├── dataflow.md         # データフロー設計
    ├── resume_and_incremental_execution.md # 再開・増分実行機能
    ├── column_lineage_specification.md # カラムリネージ仕様
    ├── development_guide.md # 開発ガイド
    ├── operations.md       # 運用ガイド
    └── workflow_patterns.md # ワークフローパターンと役割分担
```

## 開発

### テストの実行
```bash
pytest
```

### コード品質チェック
```bash
# リンティング
ruff check .

# 型チェック
mypy .

# フォーマット
black .
```

### pre-commitフックの設定
```bash
pre-commit install
```

## サンプルデータ

`data/sales_raw.csv`には以下の形式のサンプル売上データが含まれています：

| 列名 | 型 | 説明 |
|------|----|----|
| date | string | 売上日（YYYY-MM-DD） |
| product_id | string | 商品ID |
| amount | float | 売上金額 |
| customer_id | string | 顧客ID |

## 拡張性

このサンプルは以下の方向で拡張可能です：

1. **データソースの追加**: 新しいデータソース（API、データベース等）
2. **変換ロジックの追加**: 新しい分析関数をHamilton関数として追加
3. **出力形式の追加**: Parquet、データベース等への出力
4. **OpenLineage設定**: 実際のOpenLineageバックエンドとの統合

## トラブルシューティング

### よくある問題

**Q: OpenLineageの設定が見つからないエラー**
A: デフォルトではコンソール出力に設定されています。実際のOpenLineageバックエンドを使用する場合は環境変数を設定してください。

**Q: Hamiltonの型エラー**
A: 関数の入出力型が一致していることを確認してください。

**Q: データが見つからないエラー**
A: `data/sales_raw.csv`が存在することを確認してください。

## 貢献

1. フォークを作成
2. フィーチャーブランチを作成 (`git checkout -b feature/new-feature`)
3. 変更をコミット (`git commit -am 'Add new feature'`)
4. ブランチにプッシュ (`git push origin feature/new-feature`)
5. プルリクエストを作成

## ライセンス

MIT License - 詳細は[LICENSE](LICENSE)ファイルを参照してください。

## リンク

- [Hamilton Documentation](https://hamilton.dagworks.io/)
- [OpenLineage Documentation](https://openlineage.io/docs/)
- [Hamilton GitHub](https://github.com/dagworks-inc/hamilton)
- [OpenLineage GitHub](https://github.com/OpenLineage/OpenLineage)