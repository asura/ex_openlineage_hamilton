# Coding Instructions

## Naming & Structure
- Class: PascalCase (e.g., `POICountMetric`)
- Func/var/file: snake_case (`calculate_poi_count`)
- Const: UPPER_CASE (`DEFAULT_TIMEOUT_SECONDS`)
- One module = one responsibility. Avoid circular imports.

## Typing (Py ≥3.10)
- All public funcs must have type hints.
- Prefer `|` for Union / Optional. Use built-in generics (`list[str]`).
- Complex types → alias.

## Docstring
- Google style. Must include Args / Returns / Raises. Write in Japanese.

## Error & Logging
- Define custom exceptions. Use logging（DEBUG…ERROR）.

## Testing Guideline
- Use pytest-describe.
- describeはクラス→関数→場合分け(条件・状況)、と階層化する
- test_ プレフィックスなし: describe内の関数は日本語名で、その場合にどのようにふるまうかを示す
- 「～される」や「～という状態になる」ではなく「～する」と書く
- 1 assert per test.
- docstring不要: 関数名で意図を表現
- Cover 正常 / 異常 / 境界. Target ≥85 % coverage.

## git commit message
- 必ず日本語で記述する
- コミットメッセージは、最初にConventional Commitsに則って記述する
- その後にファイルごとの詳細な変更内容を記述する