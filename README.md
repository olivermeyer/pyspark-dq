# pysparkdq

`pysparkdq` is a lightweight columnar validation framework for
PySpark DataFrames.

The framework is based largely on Amazon's Deequ package; it is to some
extent a highly simplified, Python-translated Deequ minus the stateful
statistical modelling part.

The framework is built around two core concepts:
* The `CheckOperator` orchestrates and runs validations on a provided
DataFrame, returning valid and invalid rows as two DataFrames.
* The `Check` objects define columnar validations to apply to a
DataFrame.

## Usage

The framework is designed for batch processing. A typical expected usage
pattern looks like:
1. Read data from storage into a Spark DataFrame
*(not covered by the framework)*
1. Create a `CheckOperator` with this DataFrame as parameter
1. Add any number of `Check` objects to the `CheckOperator`
1. Run the validations; the `CheckOperator` returns valid and invalid
rows
1. Write invalid rows to a retention area; make valid rows available
for further processing *(not covered by the framework)*

In code:

```python
df = spark.read.parquet("s3a://path/to/input")
check_operator = CheckOperator(
	dataframe=df
)
check_operator.add_check(
	ColumnIsNotNullCheck("id"),
)
check_operator.add_check(
	ColumnIsPositiveCheck("age")
)
check_operator.add_check(
	ColumnIsInValuesCheck(
		"country", ["DE", "GB"]
	)
)
check_operator.add_check(
	ColumnSetIsUniqueCheck(
		["id", "country"]
	)
)
valid_df, invalid_df = check_operator.run_and_return()
valid_df.write.parquet("s3a://path/to/output")
invalid_df.write.parquet("s3a://path/to/retention/area")
```

See also an actual application in `example.py`.

## How it works

The output of a validation is written to the DataFrame as a boolean
field. For example, given the following DataFrame:

	| id   | name |
	|------|------|
	| 1    | Foo  |
	| 2    |      |
 
And a validation rule stating that `name` should not be null (i.e. 
`ColumnIsNotNullCheck`), the output of the validation would be:

	| id   | name | name_is_not_null |
	|------|------|------------------|
	| 1    | Foo  | True             |
	| 2    |      | False            |

The name of the additional column is defined in the corresponding
`Check` object. The additional columns are included in the invalid rows
DataFrame, but not in the valid rows DataFrame returned by the
`CheckOperator`.

## Next steps and possible improvements
* Write test cases for `CheckOperator`
* Refactor `CheckOperator` - naming and logic are not optimal
* Make the framework capable of reading/writing data; the expected gain
is to enable config-only usage
* Rename `is_positive` to `is_not_negative`
(`is_positive` is misleading for zero)
* Add logging
