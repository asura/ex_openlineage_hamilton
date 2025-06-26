"""
OpenLineageのデータ変換履歴を記録・表示するスクリプト
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from openlineage.client.event_v2 import RunEvent


class LineageHistoryRecorder:
    """OpenLineageイベントをローカルファイルに記録するクラス"""

    def __init__(self, history_dir: str = "lineage_history"):
        self.history_dir = Path(history_dir)
        self.history_dir.mkdir(exist_ok=True)

    def record_event(self, event: RunEvent):
        """イベントをJSONファイルに記録"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{event.eventType}_{timestamp}_{event.run.runId[:8]}.json"

        event_data = {
            "eventType": event.eventType,
            "eventTime": event.eventTime,
            "run": {"runId": event.run.runId, "facets": event.run.facets},
            "job": {
                "namespace": event.job.namespace,
                "name": event.job.name,
                "facets": event.job.facets,
            },
            "inputs": [
                {"namespace": ds.namespace, "name": ds.name, "facets": ds.facets}
                for ds in (event.inputs or [])
            ],
            "outputs": [
                {"namespace": ds.namespace, "name": ds.name, "facets": ds.facets}
                for ds in (event.outputs or [])
            ],
        }

        with open(self.history_dir / filename, "w", encoding="utf-8") as f:
            json.dump(event_data, f, ensure_ascii=False, indent=2)

        print(f"✅ Lineage event recorded: {filename}")


class LineageHistoryViewer:
    """記録されたリネージ履歴を表示するクラス"""

    def __init__(self, history_dir: str = "lineage_history"):
        self.history_dir = Path(history_dir)

    def list_all_events(self) -> List[Dict[str, Any]]:
        """全イベントを時系列順で取得"""
        events: List[Dict[str, Any]] = []

        if not self.history_dir.exists():
            return events

        for file_path in self.history_dir.glob("*.json"):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    event_data = json.load(f)
                    event_data["_filename"] = file_path.name
                    events.append(event_data)
            except Exception as e:
                print(f"❌ Error reading {file_path}: {e}")

        # 時間順でソート
        events.sort(key=lambda x: x.get("eventTime", ""))
        return events

    def show_pipeline_history(self):
        """パイプライン実行履歴を表示"""
        events = self.list_all_events()

        print("=" * 80)
        print("📊 OpenLineage データ変換履歴")
        print("=" * 80)

        if not events:
            print("❌ 履歴が見つかりません。パイプラインを実行してください。")
            return

        current_run = None

        for event in events:
            event_type = event.get("eventType", "UNKNOWN")
            event_time = event.get("eventTime", "")
            run_id = event.get("run", {}).get("runId", "")[:8]
            job_name = event.get("job", {}).get("name", "")

            # 新しいランの開始
            if event_type == "START":
                current_run = run_id
                print(f"\n🚀 パイプライン開始: {job_name}")
                print(f"   ⏰ 時刻: {event_time}")
                print(f"   🆔 Run ID: {run_id}")

                # 入力データセット
                inputs = event.get("inputs", [])
                if inputs:
                    print("   📥 入力データセット:")
                    for inp in inputs:
                        print(f"      - {inp.get('name', 'unknown')}")

                # 出力データセット（予定）
                outputs = event.get("outputs", [])
                if outputs:
                    print("   📤 出力データセット（予定）:")
                    for out in outputs:
                        print(f"      - {out.get('name', 'unknown')}")

            elif event_type == "COMPLETE" and run_id == current_run:
                print(f"\n✅ パイプライン完了: {job_name}")
                print(f"   ⏰ 時刻: {event_time}")

            elif event_type == "FAIL" and run_id == current_run:
                print(f"\n❌ パイプライン失敗: {job_name}")
                print(f"   ⏰ 時刻: {event_time}")

            elif event_type == "RUNNING":
                print(f"   ⚙️  実行中: {job_name} ({event_time})")

        print("\n" + "=" * 80)

    def show_data_lineage_graph(self):
        """データリネージグラフを表示"""
        events = self.list_all_events()

        print("=" * 80)
        print("🔗 データリネージグラフ")
        print("=" * 80)

        datasets = set()
        transformations = set()

        for event in events:
            inputs = event.get("inputs", [])
            outputs = event.get("outputs", [])
            job_name = event.get("job", {}).get("name", "")

            if inputs and outputs:
                transformations.add(job_name)

            for inp in inputs:
                datasets.add(inp.get("name", ""))

            for out in outputs:
                datasets.add(out.get("name", ""))

        print("📊 データセット:")
        for dataset in sorted(datasets):
            if dataset:
                print(f"   - {dataset}")

        print("\n⚙️  変換ジョブ:")
        for job in sorted(transformations):
            if job:
                print(f"   - {job}")

        print("\n📈 データフロー:")
        for event in events:
            if event.get("eventType") == "START":
                inputs = event.get("inputs", [])
                outputs = event.get("outputs", [])
                job_name = event.get("job", {}).get("name", "")

                if inputs and outputs:
                    input_names = [inp.get("name", "") for inp in inputs]
                    output_names = [out.get("name", "") for out in outputs]
                    print(
                        f"   {' + '.join(input_names)} → [{job_name}] → {' + '.join(output_names)}"
                    )

        print("=" * 80)

    def show_run_details(self, run_id_prefix: str):
        """特定の実行の詳細を表示"""
        events = self.list_all_events()

        matching_events = [
            e
            for e in events
            if e.get("run", {}).get("runId", "").startswith(run_id_prefix)
        ]

        if not matching_events:
            print(f"❌ Run ID '{run_id_prefix}' で始まる実行が見つかりません")
            return

        print("=" * 80)
        print(f"🔍 実行詳細: {run_id_prefix}")
        print("=" * 80)

        for event in matching_events:
            print(f"\n📝 イベント: {event.get('eventType')}")
            print(f"   ⏰ 時刻: {event.get('eventTime')}")
            print(f"   📁 ファイル: {event.get('_filename')}")

            # 詳細情報があれば表示
            facets = event.get("run", {}).get("facets", {})
            if facets:
                print("   📊 メタデータ:")
                for key, value in facets.items():
                    print(f"      {key}: {value}")


def main():
    """履歴表示のメイン関数"""
    import argparse

    parser = argparse.ArgumentParser(description="OpenLineage履歴表示ツール")
    parser.add_argument(
        "--command",
        "-c",
        choices=["history", "lineage", "detail"],
        default="history",
        help="表示内容を選択",
    )
    parser.add_argument("--run-id", "-r", help="詳細表示する Run ID (部分一致)")

    args = parser.parse_args()

    viewer = LineageHistoryViewer()

    if args.command == "history":
        viewer.show_pipeline_history()
    elif args.command == "lineage":
        viewer.show_data_lineage_graph()
    elif args.command == "detail" and args.run_id:
        viewer.show_run_details(args.run_id)
    else:
        print("使用例:")
        print("  python view_lineage_history.py -c history    # 実行履歴")
        print("  python view_lineage_history.py -c lineage    # リネージグラフ")
        print("  python view_lineage_history.py -c detail -r abc123  # 詳細")


if __name__ == "__main__":
    main()
