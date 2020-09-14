# pysparkdq

`pysparkdq` is a lightweight columnar validation framework for
PySpark DataFrames.

The framework is based largely on Amazon's Deequ package; it is to some
extent a highly simplified, Python-translated Deequ minus the stateful
statistical modelling part.

The framework is built around three core objects:
* The `CheckOperator` collects a DataFrame and one or more `Check`
objects, and instructs a `DataFrameValidator` to run the checks on the
DataFrame.
* The `DataFrameValidator` runs the checks on the DataFrame and returns
two DataFrames: one containing valid rows; the other invalid rows.
* The `Check` objects define columnar validations to apply to a
DataFrame.

## Usage

The framework is designed for batch processing. A typical expected usage
pattern looks like:
1. Read data from storage into a Spark DataFrame
*(not covered by the framework)*
1. Create a `CheckOperator` with this DataFrame as parameter
1. Add any number of `Check` objects to the `CheckOperator`
1. Run the checks
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
valid_df, invalid_df = check_operator.run()
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
`DataFrameValidator`.

## Next steps and possible improvements
* Write test cases for `CheckOperator`
* Make the framework capable of reading/writing data; the expected gain
is to enable config-only usage
* Rename `is_positive` to `is_not_negative`
(`is_positive` is misleading for zero)
* Add logging
