# ワークフローパターンと役割分担

## 概要

このドキュメントでは、Hamilton、OpenLineage、Airflowの役割分担と、長時間処理における進捗把握のパターンについて説明します。

## ライブラリ役割比較

### Hamilton vs Airflow

| 側面 | Hamilton | Airflow |
|------|----------|---------|
| **DAG定義** | 関数シグネチャから自動推論 | PythonでDAG明示的定義 |
| **依存関係** | 引数名による自動解決 | 手動で`>>`等で定義 |
| **スケジューリング** | ✗ なし | ◎ 強力（cron、センサー等） |
| **UI・監視** | △ 基本的な可視化のみ | ◎ 豊富なWeb UI |
| **データフロー** | ◎ 型安全なデータフロー | △ タスク間のデータ受け渡し複雑 |
| **テスト** | ◎ 純粋関数で単体テスト容易 | △ 統合テスト中心 |
| **学習コスト** | ○ 関数ベースで直感的 | △ 概念とAPI学習が必要 |
| **デバッグ** | ◎ 関数単位で実行・検証 | △ DAG全体の実行が基本 |

### 進捗監視の責務

| 進捗の側面 | Hamilton | OpenLineage | 外部オーケストレーター |
|----------|----------|-------------|---------------------|
| **実行進捗** | ◎ 主責務 | △ 補助（イベント記録） | ◎ UI提供 |
| **ステップ監視** | ◎ 実行制御 | ◎ ログ記録 | ◎ 可視化 |
| **データリネージ** | △ 補助 | ◎ 主責務 | - |
| **外部通知** | - | ◎ イベント送信 | ◎ アラート機能 |
| **履歴管理** | - | ◎ 永続化 | ◎ データベース |

## ワークフローパターン

### Pattern 1: Hamilton + OpenLineage（データ変換中心）

```python
# 推奨ユースケース：データ分析、機械学習パイプライン、研究開発

# Hamilton関数でデータ変換ロジック
def raw_data() -> pd.DataFrame:
    return pd.read_csv("input.csv")

def cleaned_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    return raw_data.dropna()

def analysis_result(cleaned_data: pd.DataFrame) -> Dict[str, Any]:
    return {"mean": cleaned_data.mean(), "std": cleaned_data.std()}

# OpenLineage統合実行
def run_analysis_pipeline():
    tracker = OpenLineageHamiltonTracker()
    dr = driver.Driver({}, analysis_modules)
    
    with openlineage_context(tracker):
        results = dr.execute(["analysis_result"])
    
    return results
```

**適用場面**:
- ✅ データ変換ロジックが複雑
- ✅ 型安全性を重視
- ✅ データサイエンス・分析チーム主体
- ✅ アドホック実行が多い
- ✅ 単体テストを重視

**長所**:
- 関数の依存関係が自動解決される
- 純粋関数なので単体テストが容易
- 型ヒントによる安全性
- データリネージの自動追跡

**制約**:
- スケジューリング機能なし
- 監視UIが基本的
- 外部システム連携は自分で実装

### Pattern 2: Airflow + OpenLineage（運用中心）

```python
# 推奨ユースケース：本格ETL、定期バッチ処理、運用システム

@dag(schedule_interval="@daily", start_date=datetime(2024, 1, 1))
def etl_pipeline_dag():
    
    @task
    def extract_from_source():
        # OpenLineageで自動追跡
        with openlineage_context("extract"):
            return extract_sales_data()
    
    @task
    def transform_data(raw_data):
        with openlineage_context("transform"):
            return apply_business_rules(raw_data)
    
    @task
    def load_to_warehouse(transformed_data):
        with openlineage_context("load"):
            return upload_to_bigquery(transformed_data)
    
    # 外部依存の待機
    wait_for_upstream = ExternalTaskSensor(
        external_dag_id="upstream_dag",
        external_task_id="prepare_data"
    )
    
    # 通知
    notify_completion = EmailOperator(
        task_id="send_completion_email",
        to=["team@company.com"],
        subject="ETL Pipeline Completed"
    )
    
    # DAG構築
    extract = extract_from_source()
    transform = transform_data(extract)
    load = load_to_warehouse(transform)
    
    wait_for_upstream >> extract >> transform >> load >> notify_completion
```

**適用場面**:
- ✅ 定期スケジューリングが必要
- ✅ 外部システム連携が多い
- ✅ 監視・アラートが重要
- ✅ 運用チームによる管理
- ✅ SLA管理が必要

**長所**:
- 豊富なスケジューリング機能
- Web UIによる監視・操作
- 豊富なオペレーター（DB、API、クラウド等）
- 運用実績が豊富

**制約**:
- タスク間のデータ受け渡しが複雑
- 単体テストが困難
- 学習コスト高

### Pattern 3: Airflow + Hamilton + OpenLineage（ハイブリッド）

```python
# 推奨ユースケース：複雑なデータ変換を含む運用パイプライン

@dag(schedule_interval="@daily")
def hybrid_pipeline_dag():
    
    # 外部依存・前処理はAirflow
    wait_for_data = ExternalTaskSensor(
        external_dag_id="data_preparation",
        external_task_id="export_complete"
    )
    
    validate_input = BashOperator(
        task_id="validate_input_files",
        bash_command="python validate_inputs.py"
    )
    
    # 複雑なデータ変換はHamilton
    @task
    def run_hamilton_analysis():
        """Hamilton + OpenLineageでデータ変換"""
        tracker = OpenLineageHamiltonTracker("hybrid_pipeline")
        dr = driver.Driver({}, analysis_modules)
        
        try:
            tracker.emit_start_event(...)
            
            # Hamilton DAG実行
            results = dr.execute([
                "cleaned_data",
                "feature_engineering", 
                "model_predictions",
                "business_metrics"
            ])
            
            tracker.emit_complete_event(...)
            return results
            
        except Exception as e:
            tracker.emit_fail_event(...)
            raise
    
    # 後処理・通知はAirflow
    @task
    def publish_results(hamilton_results):
        upload_to_dashboard(hamilton_results)
        return "success"
    
    send_alerts = EmailOperator(
        task_id="send_completion_alert",
        to=["stakeholders@company.com"]
    )
    
    # DAG構築
    hamilton_task = run_hamilton_analysis()
    publish_task = publish_results(hamilton_task)
    
    wait_for_data >> validate_input >> hamilton_task >> publish_task >> send_alerts
```

**適用場面**:
- ✅ 複雑なデータ変換 + 運用要件
- ✅ 開発効率と運用性の両立
- ✅ データサイエンスチーム + 運用チーム協業
- ✅ 段階的な移行戦略

**長所**:
- Hamiltonの開発効率 + Airflowの運用機能
- 責務の明確な分離
- 既存Airflow資産の活用

**制約**:
- アーキテクチャが複雑
- 両方のライブラリの学習が必要

## 長時間処理における進捗監視

### Hamilton中心の進捗実装

```python
class ProgressiveHamiltonRunner:
    """長時間Hamilton処理での進捗監視"""
    
    def __init__(self):
        self.tracker = OpenLineageHamiltonTracker()
        self.progress_callbacks = []
        
    def add_progress_callback(self, callback):
        """進捗通知コールバックを追加"""
        self.progress_callbacks.append(callback)
        
    def notify_progress(self, step: str, progress: float, message: str):
        """進捗を通知"""
        for callback in self.progress_callbacks:
            callback(step, progress, message)
    
    def run_with_progress(self, outputs: List[str]):
        """進捗監視付きでHamilton実行"""
        dr = driver.Driver({}, pipeline_modules)
        
        # 実行プランの取得
        execution_plan = dr.what_is_downstream_of(outputs)
        total_steps = len(execution_plan)
        
        self.tracker.emit_start_event(...)
        
        try:
            results = {}
            for i, step in enumerate(execution_plan):
                # 進捗通知
                progress_pct = (i / total_steps) * 100
                self.notify_progress(step, progress_pct, f"Executing {step}")
                
                # OpenLineageステップ開始
                self.tracker.emit_step_start(step)
                
                # Hamilton実行
                step_result = dr.execute([step])
                results.update(step_result)
                
                # OpenLineageステップ完了
                self.tracker.emit_step_complete(step, get_metrics(step_result))
                
            self.tracker.emit_complete_event(...)
            return results
            
        except Exception as e:
            self.tracker.emit_fail_event(...)
            raise

# 使用例
def run_long_analysis():
    runner = ProgressiveHamiltonRunner()
    
    # Slack通知
    runner.add_progress_callback(
        lambda step, pct, msg: slack_notify(f"Pipeline {pct:.1f}%: {msg}")
    )
    
    # ローカル進捗バー
    runner.add_progress_callback(
        lambda step, pct, msg: print(f"[{'=' * int(pct/5)}>{' ' * (20-int(pct/5))}] {pct:.1f}%")
    )
    
    return runner.run_with_progress(["final_report"])
```

### 外部監視システムとの統合

```python
class ExternalMonitoringIntegration:
    """外部監視システム（Prometheus、DataDog等）との統合"""
    
    def __init__(self):
        self.metrics_client = PrometheusClient()
        self.openlineage_client = OpenLineageClient()
    
    def emit_step_metrics(self, step_name: str, execution_time: float, record_count: int):
        """ステップ実行メトリクスを送信"""
        # Prometheus
        self.metrics_client.histogram("hamilton_step_duration").observe(execution_time)
        self.metrics_client.gauge("hamilton_records_processed").set(record_count)
        
        # OpenLineage
        self.openlineage_client.emit(RunEvent(
            eventType="RUNNING",
            run=self.current_run,
            job=self.create_job(step_name),
            facets={
                "performance": {
                    "execution_time_seconds": execution_time,
                    "records_processed": record_count
                }
            }
        ))

# ダッシュボード用のメトリクス例
"""
Grafana Dashboard設定例：

1. Pipeline進捗率
   Query: hamilton_pipeline_progress_percent

2. ステップ実行時間
   Query: hamilton_step_duration

3. 処理レコード数
   Query: hamilton_records_processed

4. エラー率
   Query: rate(hamilton_step_errors_total[5m])
"""
```

## 選択指針

### プロジェクト特性による選択

| プロジェクト特性 | 推奨パターン | 理由 |
|-----------------|-------------|------|
| **研究・分析プロジェクト** | Hamilton + OpenLineage | 柔軟性、テスト容易性 |
| **本格運用ETL** | Airflow + OpenLineage | スケジューリング、監視 |
| **ML模型学習パイプライン** | Hamilton + OpenLineage | 複雑な依存関係、再現性 |
| **定期レポート生成** | Airflow + OpenLineage | 定期実行、通知 |
| **アドホック分析** | Hamilton + OpenLineage | 即座実行、試行錯誤 |
| **データ品質監視** | Airflow + OpenLineage | 継続監視、アラート |

### チーム特性による選択

| チーム特性 | 推奨パターン | 配慮点 |
|-----------|-------------|--------|
| **データサイエンティスト中心** | Hamilton + OpenLineage | 関数型思考、Jupyter親和性 |
| **データエンジニア中心** | Airflow + OpenLineage | 運用経験、インフラ知識 |
| **混成チーム** | ハイブリッド | 責務分離、段階移行 |

### 技術的制約による選択

| 制約 | パターン | 対策 |
|------|---------|------|
| **既存Airflow環境** | ハイブリッド | 段階的Hamilton導入 |
| **クラウドネイティブ** | Airflow + OpenLineage | マネージドサービス活用 |
| **オンプレミス** | Hamilton + OpenLineage | シンプルな構成 |
| **コンテナ環境** | どれでも可 | 適切なイメージ作成 |

## 実装のベストプラクティス

### 1. 段階的導入

```python
# Phase 1: Hamilton単体
dr = driver.Driver({}, modules)
results = dr.execute(["output"])

# Phase 2: OpenLineage追加
with openlineage_tracking():
    results = dr.execute(["output"])

# Phase 3: 進捗監視追加
runner = ProgressiveHamiltonRunner()
results = runner.run_with_progress(["output"])

# Phase 4: Airflow統合（必要に応じて）
@task
def hamilton_task():
    return runner.run_with_progress(["output"])
```

### 2. 監視・アラート戦略

```python
def setup_comprehensive_monitoring():
    """包括的な監視設定"""
    
    # 1. アプリケーションレベル（Hamilton）
    setup_hamilton_callbacks()
    
    # 2. データリネージレベル（OpenLineage）
    setup_openlineage_events()
    
    # 3. インフラレベル（Prometheus等）
    setup_system_metrics()
    
    # 4. ビジネスレベル（カスタム）
    setup_business_alerts()
```

### 3. エラー処理・復旧

```python
class ResilientPipelineRunner:
    """障害耐性のあるパイプライン実行"""
    
    def run_with_retry(self, outputs, max_retries=3):
        for attempt in range(max_retries):
            try:
                return self.run_with_progress(outputs)
            except Exception as e:
                if attempt == max_retries - 1:
                    self.notify_failure(e)
                    raise
                else:
                    self.notify_retry(attempt + 1, e)
                    time.sleep(2 ** attempt)  # exponential backoff
```

## 関連ドキュメント
- [アーキテクチャ設計](architecture.md)
- [データフロー設計](dataflow.md)
- [再開・増分実行機能](resume_and_incremental_execution.md)
- [カラムリネージ仕様](column_lineage_specification.md)
- [開発ガイド](development_guide.md)
- [運用ガイド](operations.md)
- [README](../README.md)