"""
カラムレベルのデータリネージ追跡
Hamilton関数の入出力カラムを詳細に記録する
"""

import json
import inspect
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Callable
from dataclasses import dataclass, asdict
from functools import wraps


@dataclass
class ColumnLineageRecord:
    """カラムリネージの記録"""

    timestamp: str
    function_name: str
    input_columns: Dict[str, List[str]]  # {dataset_name: [column_names]}
    output_columns: Dict[str, List[str]]  # {dataset_name: [column_names]}
    column_transformations: List[Dict[str, Any]]  # 詳細な変換記録
    operation_type: str  # CREATE, READ, UPDATE, DELETE
    data_types: Dict[str, str]  # カラムのデータ型


@dataclass
class ColumnTransformation:
    """個別カラム変換の詳細"""

    source_dataset: str
    source_column: str
    target_dataset: str
    target_column: str
    transformation_logic: str
    function_name: str
    operation: str  # derived, copied, aggregated, filtered, etc.


class ColumnLineageTracker:
    """カラムレベルのリネージ追跡クラス"""

    def __init__(self, lineage_dir: str = "column_lineage"):
        self.lineage_dir = Path(lineage_dir)
        self.lineage_dir.mkdir(exist_ok=True)
        self.current_function = None
        self.transformations: List[ColumnTransformation] = []

    def track_function(self, func: Callable) -> Callable:
        """Hamilton関数をデコレートしてカラムリネージを追跡"""

        @wraps(func)
        def wrapper(*args, **kwargs):
            self.current_function = func.__name__

            # 入力の分析
            input_analysis = self._analyze_inputs(func, args, kwargs)

            # 関数実行
            result = func(*args, **kwargs)

            # 出力の分析
            output_analysis = self._analyze_output(func.__name__, result)

            # リネージ記録
            self._record_lineage(
                function_name=func.__name__,
                input_analysis=input_analysis,
                output_analysis=output_analysis,
                func_source=inspect.getsource(func),
            )

            return result

        return wrapper

    def _analyze_inputs(
        self, func: Callable, args: tuple, kwargs: dict
    ) -> Dict[str, Any]:
        """関数の入力を分析"""
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        input_analysis: Dict[str, Any] = {"datasets": {}, "parameters": {}}

        for param_name, value in bound_args.arguments.items():
            if isinstance(value, pd.DataFrame):
                input_analysis["datasets"][param_name] = {
                    "columns": list(value.columns),
                    "dtypes": {col: str(dtype) for col, dtype in value.dtypes.items()},
                    "shape": value.shape,
                    "sample_values": self._get_sample_values(value),
                }
            else:
                input_analysis["parameters"][param_name] = {
                    "type": type(value).__name__,
                    "value": str(value)[:100],  # 最初の100文字のみ
                }

        return input_analysis

    def _analyze_output(self, func_name: str, result: Any) -> Dict[str, Any]:
        """関数の出力を分析"""
        output_analysis: Dict[str, Any] = {
            "type": type(result).__name__,
            "datasets": {},
            "values": {},
        }

        if isinstance(result, pd.DataFrame):
            output_analysis["datasets"][func_name] = {
                "columns": list(result.columns),
                "dtypes": {col: str(dtype) for col, dtype in result.dtypes.items()},
                "shape": result.shape,
                "sample_values": self._get_sample_values(result),
            }
        elif isinstance(result, dict):
            output_analysis["values"] = {
                "keys": list(result.keys()),
                "sample": {k: str(v)[:100] for k, v in list(result.items())[:5]},
            }
        elif isinstance(result, list):
            output_analysis["values"] = {
                "length": len(result),
                "sample": [str(item)[:100] for item in result[:3]],
            }

        return output_analysis

    def _get_sample_values(
        self, df: pd.DataFrame, n_samples: int = 3
    ) -> Dict[str, List[Any]]:
        """DataFrameからサンプル値を取得"""
        sample_values = {}
        for col in df.columns:
            try:
                # null値以外の値から最大n_samples個取得
                non_null_values = df[col].dropna()
                if len(non_null_values) > 0:
                    samples = non_null_values.head(n_samples).tolist()
                    sample_values[col] = [str(val) for val in samples]
                else:
                    sample_values[col] = ["<all null>"]
            except Exception:
                sample_values[col] = ["<error reading>"]

        return sample_values

    def _record_lineage(
        self,
        function_name: str,
        input_analysis: Dict,
        output_analysis: Dict,
        func_source: str,
    ):
        """リネージ記録をファイルに保存"""
        timestamp = datetime.now().isoformat()

        # カラム変換の推論
        column_transformations = self._infer_column_transformations(
            function_name, input_analysis, output_analysis, func_source
        )

        lineage_record = ColumnLineageRecord(
            timestamp=timestamp,
            function_name=function_name,
            input_columns={
                name: data["columns"]
                for name, data in input_analysis["datasets"].items()
            },
            output_columns={
                name: data["columns"]
                for name, data in output_analysis["datasets"].items()
            },
            column_transformations=column_transformations,
            operation_type=self._infer_operation_type(
                function_name, input_analysis, output_analysis
            ),
            data_types=self._extract_data_types(input_analysis, output_analysis),
        )

        # 詳細分析も保存
        detailed_record = {
            "lineage_summary": asdict(lineage_record),
            "input_analysis": input_analysis,
            "output_analysis": output_analysis,
            "function_source": func_source,
        }

        filename = f"{function_name}_{timestamp.replace(':', '-')}.json"
        with open(self.lineage_dir / filename, "w", encoding="utf-8") as f:
            json.dump(detailed_record, f, ensure_ascii=False, indent=2)

        print(f"🔍 Column lineage recorded: {filename}")

    def _infer_column_transformations(
        self,
        func_name: str,
        input_analysis: Dict,
        output_analysis: Dict,
        func_source: str,
    ) -> List[Dict[str, Any]]:
        """カラム変換の推論"""
        transformations = []

        input_datasets = input_analysis["datasets"]
        output_datasets = output_analysis["datasets"]

        for output_name, output_data in output_datasets.items():
            for output_col in output_data["columns"]:
                # 入力カラムとの関係を推論
                possible_sources = []

                for input_name, input_data in input_datasets.items():
                    for input_col in input_data["columns"]:
                        # 名前の類似性で推論
                        if self._are_columns_related(
                            input_col, output_col, func_source
                        ):
                            possible_sources.append(
                                {
                                    "source_dataset": input_name,
                                    "source_column": input_col,
                                    "target_dataset": output_name,
                                    "target_column": output_col,
                                    "transformation_logic": self._extract_transformation_logic(
                                        input_col, output_col, func_source
                                    ),
                                    "function_name": func_name,
                                    "operation": self._infer_operation_type_for_column(
                                        input_col, output_col, func_source
                                    ),
                                    "confidence": self._calculate_confidence(
                                        input_col, output_col, func_source
                                    ),
                                }
                            )

                if not possible_sources:
                    # 新規作成カラムの可能性
                    transformations.append(
                        {
                            "source_dataset": None,
                            "source_column": None,
                            "target_dataset": output_name,
                            "target_column": output_col,
                            "transformation_logic": self._extract_creation_logic(
                                output_col, func_source
                            ),
                            "function_name": func_name,
                            "operation": "created",
                            "confidence": 0.8,
                        }
                    )
                else:
                    transformations.extend(possible_sources)

        return transformations

    def _are_columns_related(
        self, input_col: str, output_col: str, func_source: str
    ) -> bool:
        """入力と出力カラムの関連性を判断"""
        # 完全一致
        if input_col == output_col:
            return True

        # 部分一致
        if input_col in output_col or output_col in input_col:
            return True

        # 関数ソースコード内での言及
        if input_col in func_source and output_col in func_source:
            return True

        return False

    def _extract_transformation_logic(
        self, input_col: str, output_col: str, func_source: str
    ) -> str:
        """変換ロジックの抽出"""
        # ソースコードから関連する行を抽出
        lines = func_source.split("\n")
        relevant_lines = []

        for line in lines:
            if input_col in line or output_col in line:
                relevant_lines.append(line.strip())

        return " | ".join(relevant_lines[:3])  # 最初の3行まで

    def _extract_creation_logic(self, output_col: str, func_source: str) -> str:
        """新規作成ロジックの抽出"""
        lines = func_source.split("\n")
        for line in lines:
            if output_col in line and ("=" in line or "agg(" in line):
                return line.strip()
        return "Column created by function logic"

    def _infer_operation_type(
        self, func_name: str, input_analysis: Dict, output_analysis: Dict
    ) -> str:
        """操作タイプの推論"""
        has_input = len(input_analysis["datasets"]) > 0
        has_output = len(output_analysis["datasets"]) > 0

        if not has_input and has_output:
            return "CREATE"
        elif has_input and has_output:
            return "UPDATE"
        elif has_input and not has_output:
            return "READ"
        else:
            return "UNKNOWN"

    def _infer_operation_type_for_column(
        self, input_col: str, output_col: str, func_source: str
    ) -> str:
        """カラム単位の操作タイプ推論"""
        if input_col == output_col:
            return "copied"
        elif "groupby" in func_source and "agg" in func_source:
            return "aggregated"
        elif "rolling" in func_source:
            return "windowed"
        elif "pct_change" in func_source:
            return "derived"
        else:
            return "transformed"

    def _calculate_confidence(
        self, input_col: str, output_col: str, func_source: str
    ) -> float:
        """変換の信頼度計算"""
        confidence = 0.5

        if input_col == output_col:
            confidence += 0.4
        elif input_col in output_col:
            confidence += 0.3

        if input_col in func_source and output_col in func_source:
            confidence += 0.2

        return min(confidence, 1.0)

    def _extract_data_types(
        self, input_analysis: Dict, output_analysis: Dict
    ) -> Dict[str, str]:
        """データ型の抽出"""
        data_types = {}

        for name, data in input_analysis["datasets"].items():
            for col, dtype in data["dtypes"].items():
                data_types[f"{name}.{col}"] = dtype

        for name, data in output_analysis["datasets"].items():
            for col, dtype in data["dtypes"].items():
                data_types[f"{name}.{col}"] = dtype

        return data_types


class ColumnLineageViewer:
    """カラムリネージの表示クラス"""

    def __init__(self, lineage_dir: str = "column_lineage"):
        self.lineage_dir = Path(lineage_dir)

    def show_column_lineage_table(self):
        """CRUD表形式でカラムリネージを表示"""
        records = self._load_all_records()

        print("=" * 120)
        print("📊 カラムレベルデータリネージ（CRUD表）")
        print("=" * 120)
        print(
            f"{'Function':<20} {'Operation':<10} {'Source':<30} {'Target':<30} {'Transformation':<25}"
        )
        print("-" * 120)

        for record in records:
            lineage = record["lineage_summary"]
            for transform in lineage["column_transformations"]:
                source = (
                    f"{transform['source_dataset']}.{transform['source_column']}"
                    if transform["source_dataset"]
                    else "NEW"
                )
                target = f"{transform['target_dataset']}.{transform['target_column']}"

                print(
                    f"{transform['function_name']:<20} {transform['operation']:<10} {source:<30} {target:<30} {transform['transformation_logic'][:25]:<25}"
                )

    def show_column_journey(self, column_pattern: str):
        """特定カラムの変換履歴を表示"""
        records = self._load_all_records()

        print("=" * 100)
        print(f"🔍 カラム変換履歴: '{column_pattern}'")
        print("=" * 100)

        journey = []
        for record in records:
            lineage = record["lineage_summary"]
            for transform in lineage["column_transformations"]:
                source_col = transform.get("source_column", "")
                target_col = transform.get("target_column", "")

                if (
                    (source_col and column_pattern in source_col)
                    or (target_col and column_pattern in target_col)
                    or (column_pattern in transform.get("target_dataset", ""))
                ):
                    journey.append(
                        {
                            "timestamp": lineage["timestamp"],
                            "function": transform["function_name"],
                            "source": f"{transform.get('source_dataset', 'NEW')}.{source_col}",
                            "target": f"{transform['target_dataset']}.{target_col}",
                            "operation": transform["operation"],
                            "logic": transform["transformation_logic"],
                        }
                    )

        # 時系列順でソート
        journey.sort(key=lambda x: x["timestamp"])

        for i, step in enumerate(journey):
            print(f"Step {i+1}: {step['function']}")
            print(f"  {step['source']} → {step['target']}")
            print(f"  Operation: {step['operation']}")
            print(f"  Logic: {step['logic']}")
            print(f"  Time: {step['timestamp']}")
            print()

    def show_function_impact(self, function_name: str):
        """関数の影響範囲を表示"""
        records = self._load_all_records()

        print("=" * 100)
        print(f"⚙️  関数影響分析: '{function_name}'")
        print("=" * 100)

        for record in records:
            lineage = record["lineage_summary"]
            if lineage["function_name"] == function_name:
                print(f"実行時刻: {lineage['timestamp']}")
                print(f"操作タイプ: {lineage['operation_type']}")

                print("\n📥 入力カラム:")
                for dataset, columns in lineage["input_columns"].items():
                    print(f"  {dataset}: {', '.join(columns)}")

                print("\n📤 出力カラム:")
                for dataset, columns in lineage["output_columns"].items():
                    print(f"  {dataset}: {', '.join(columns)}")

                print("\n🔄 変換詳細:")
                for transform in lineage["column_transformations"]:
                    source = f"{transform.get('source_dataset', 'NEW')}.{transform.get('source_column', '')}"
                    target = (
                        f"{transform['target_dataset']}.{transform['target_column']}"
                    )
                    print(f"  {source} → {target} ({transform['operation']})")
                print("-" * 80)

    def _load_all_records(self) -> List[Dict]:
        """全記録を読み込み"""
        records: List[Dict] = []

        if not self.lineage_dir.exists():
            return records

        for file_path in self.lineage_dir.glob("*.json"):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    records.append(json.load(f))
            except Exception as e:
                print(f"❌ Error reading {file_path}: {e}")

        return records


def main():
    """カラムリネージ表示のメイン関数"""
    import argparse

    parser = argparse.ArgumentParser(description="カラムレベルリネージ表示ツール")
    parser.add_argument(
        "--command",
        "-c",
        choices=["table", "journey", "function"],
        default="table",
        help="表示内容を選択",
    )
    parser.add_argument("--column", "-col", help="カラム名パターン（journey用）")
    parser.add_argument("--function", "-f", help="関数名（function用）")

    args = parser.parse_args()

    viewer = ColumnLineageViewer()

    if args.command == "table":
        viewer.show_column_lineage_table()
    elif args.command == "journey" and args.column:
        viewer.show_column_journey(args.column)
    elif args.command == "function" and args.function:
        viewer.show_function_impact(args.function)
    else:
        print("使用例:")
        print("  python column_lineage_tracker.py -c table                    # CRUD表")
        print(
            "  python column_lineage_tracker.py -c journey -col amount      # カラム履歴"
        )
        print(
            "  python column_lineage_tracker.py -c function -f cleaned_sales_data  # 関数影響"
        )


if __name__ == "__main__":
    main()
