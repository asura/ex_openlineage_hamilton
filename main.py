"""
OpenLineage統合のメイン実行スクリプト
HamiltonのDAGを実行し、OpenLineageでデータリネージを追跡する
"""

import json
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from hamilton import driver
from hamilton.base import SimplePythonGraphAdapter
from openlineage.client import OpenLineageClient
from view_lineage_history import LineageHistoryRecorder
from openlineage.client.event_v2 import RunEvent, Job, Run, Dataset
from openlineage.client.uuid import generate_new_uuid

import pipeline_step1
import pipeline_step2


class NumpyEncoder(json.JSONEncoder):
    """NumPy型をJSON serializable に変換するエンコーダー"""

    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


class OpenLineageHamiltonTracker:
    """
    HamiltonパイプラインのOpenLineage統合クラス
    """

    def __init__(self, namespace: str = "hamilton_pipeline"):
        """
        初期化

        Args:
            namespace: OpenLineageのネームスペース
        """
        self.namespace = namespace
        self.client = OpenLineageClient()
        self.run_id = generate_new_uuid()
        self.history_recorder = LineageHistoryRecorder()

    def create_job(self, job_name: str) -> Job:
        """
        Jobオブジェクトを作成する

        Args:
            job_name: ジョブ名

        Returns:
            Job: OpenLineageのJobオブジェクト
        """
        return Job(namespace=self.namespace, name=job_name, facets={})

    def create_run(self) -> Run:
        """
        Runオブジェクトを作成する

        Returns:
            Run: OpenLineageのRunオブジェクト
        """
        return Run(runId=str(self.run_id), facets={})

    def create_dataset(self, name: str) -> Dataset:
        """
        Datasetオブジェクトを作成する

        Args:
            name: データセット名

        Returns:
            Dataset: OpenLineageのDatasetオブジェクト
        """
        return Dataset(namespace=self.namespace, name=name, facets={})

    def emit_start_event(
        self,
        job: Job,
        run: Run,
        inputs: Optional[List] = None,
        outputs: Optional[List] = None,
    ):
        """
        ジョブ開始イベントを送信する

        Args:
            job: Jobオブジェクト
            run: Runオブジェクト
            inputs: 入力データセットのリスト
            outputs: 出力データセットのリスト
        """
        event = RunEvent(
            eventType="START",  # type: ignore
            eventTime=datetime.now().isoformat(),
            run=run,
            job=job,
            inputs=inputs or [],
            outputs=outputs or [],
        )

        self.client.emit(event)
        self.history_recorder.record_event(event)
        print(f"START event emitted for job: {job.name}")

    def emit_complete_event(
        self,
        job: Job,
        run: Run,
        inputs: Optional[List] = None,
        outputs: Optional[List] = None,
    ):
        """
        ジョブ完了イベントを送信する

        Args:
            job: Jobオブジェクト
            run: Runオブジェクト
            inputs: 入力データセットのリスト
            outputs: 出力データセットのリスト
        """
        event = RunEvent(
            eventType="COMPLETE",  # type: ignore
            eventTime=datetime.now().isoformat(),
            run=run,
            job=job,
            inputs=inputs or [],
            outputs=outputs or [],
        )

        self.client.emit(event)
        self.history_recorder.record_event(event)
        print(f"COMPLETE event emitted for job: {job.name}")


def run_pipeline_with_openlineage():
    """
    OpenLineage統合でHamiltonパイプラインを実行する
    """
    # OpenLineageトラッカーを初期化
    tracker = OpenLineageHamiltonTracker()

    # Hamilton Driverを初期化
    adapter = SimplePythonGraphAdapter()
    dr = driver.Driver({}, pipeline_step1, pipeline_step2, adapter=adapter)

    # ジョブとランを作成
    job = tracker.create_job("sales_data_pipeline")
    run = tracker.create_run()

    # 入力データセット定義
    input_dataset = tracker.create_dataset("sales_raw_csv")

    # 出力データセット定義
    output_datasets = [
        tracker.create_dataset("daily_sales_summary"),
        tracker.create_dataset("weekly_aggregation"),
        tracker.create_dataset("trend_analysis_results"),
        tracker.create_dataset("data_quality_metrics"),
    ]

    try:
        # パイプライン開始イベントを送信
        tracker.emit_start_event(
            job=job, run=run, inputs=[input_dataset], outputs=output_datasets
        )

        # Hamiltonパイプラインを実行（個別に実行）
        print("Executing Hamilton pipeline...")

        # 結果を個別に取得
        results = {}

        # DataFrame系の結果
        df_outputs = [
            "daily_sales_summary",
            "enriched_daily_summary",
            "weekly_aggregation",
        ]
        for output in df_outputs:
            results[output] = dr.execute([output])[output]

        # dict/list系の結果
        dict_outputs = ["trend_analysis", "top_performing_days", "data_quality_metrics"]
        for output in dict_outputs:
            results[output] = dr.execute([output])[output]

        # 結果を保存
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        # DataFrameはCSVで保存
        for key, value in results.items():
            if hasattr(value, "to_csv"):  # DataFrame
                value.to_csv(output_dir / f"{key}.csv", index=False)
                print(f"Saved {key}.csv")
            else:  # Dict, List, etc.
                with open(output_dir / f"{key}.json", "w", encoding="utf-8") as f:
                    json.dump(value, f, ensure_ascii=False, indent=2, cls=NumpyEncoder)
                print(f"Saved {key}.json")

        # パイプライン完了イベントを送信
        tracker.emit_complete_event(
            job=job, run=run, inputs=[input_dataset], outputs=output_datasets
        )

        print("Pipeline completed successfully!")
        return results

    except Exception as e:
        # エラーイベントを送信
        error_event = RunEvent(
            eventType="FAIL",  # type: ignore
            eventTime=datetime.now().isoformat(),
            run=run,
            job=job,
            inputs=[input_dataset],
            outputs=output_datasets,
        )
        tracker.client.emit(error_event)
        tracker.history_recorder.record_event(error_event)

        print(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # データディレクトリを作成
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    # パイプラインを実行
    results = run_pipeline_with_openlineage()

    # 結果をサマリー表示
    print("\n=== Pipeline Results Summary ===")
    for key, value in results.items():
        if hasattr(value, "shape"):  # DataFrame
            print(f"{key}: {value.shape[0]} rows, {value.shape[1]} columns")
        elif isinstance(value, dict):
            print(f"{key}: {len(value)} metrics")
        elif isinstance(value, list):
            print(f"{key}: {len(value)} items")
        else:
            print(f"{key}: {type(value)}")
