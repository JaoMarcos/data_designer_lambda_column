# Data Designer Lambda Column Plugin

A plugin for [`data-designer`](https://github.com/NVIDIA-NeMo/DataDesigner) that allows you to define columns using custom Python functions. This enables you to inject logic, transformations, and computations directly into your data generation pipeline.

## Features

- **Row-wise Operations**: Apply a function to each row (similar to `pandas.DataFrame.apply(axis=1)`).
- **Full DataFrame Operations**: Apply transformations to the entire DataFrame (e.g., exploding lists, aggregations, filtering, pivoting).
- **Dependency Management**: Explicitly declare required columns to ensure execution order.

## Installation

This plugin is designed to be used with `data-designer`.

```bash
pip install data-designer-lambda-column
```

## Usage

### Basic Row-wise Transformation

Use `operation_type="row"` (default) to calculate values based on other columns in the same row.

```python
from data_designer_lambda_column.plugin import LambdaColumnConfig
from data_designer.essentials import DataDesignerConfigBuilder, SamplerColumnConfig, CategorySamplerParams

builder = DataDesignerConfigBuilder()

# 1. Add some base data
builder.add_column(
    SamplerColumnConfig(
        name="quantity",
        sampler_type="category",
        params=CategorySamplerParams(values=[10, 20, 30]),
    )
)

builder.add_column(
    SamplerColumnConfig(
        name="price",
        sampler_type="category",
        params=CategorySamplerParams(values=[5.0, 10.0]),
    )
)

# 2. Add a computed column using a lambda function
builder.add_column(
    LambdaColumnConfig(
        name="total_cost",
        required_cols=["quantity", "price"],
        operation_type="row",  # default
        column_function=lambda row: row["quantity"] * row["price"]
    )
)
```

### Advanced Full DataFrame Transformation

Use `operation_type="full"` when you need to change the shape of the DataFrame (e.g., `explode`, `melt`) or perform operations that require the full context.

**Note**: When using `operation_type="full"`, your function receives the entire DataFrame and **must return the modified DataFrame**.

> **Warning**: Operations that change the number of rows (like `explode`) may not work as expected in the current version due to validation checks on update records in `data_designer`.


```python
from data_designer_lambda_column.plugin import LambdaColumnConfig
from data_designer.essentials import DataDesignerConfigBuilder

# Define a function to explode a list column
def explode_items(df):
    # Assume 'items_list' is a column containing lists of items
    # e.g., [['apple', 'banana'], ['orange']]
    
    # Explode the list so each item gets its own row
    expanded_df = df.explode("items_list")
    
    # Ensure dependencies are met
    # The new column name 'single_item' must exist in the returned DataFrame
    expanded_df["single_item"] = expanded_df["items_list"]
    
    return expanded_df

builder.add_column(
    LambdaColumnConfig(
        name="single_item",
        required_cols=["items_list"],
        operation_type="full",
        column_function=explode_items
    )
)
```

## Configuration

`LambdaColumnConfig` accepts the following parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | Required | The name of the column to generate. |
| `column_function` | `callable` | Required | The Python function to execute. |
| `required_cols` | `list[str]` | `[]` | List of column names that must exist before this column is generated. |
| `operation_type` | `Literal["row", "full"]` | `"row"` | Type of operation. `"row"` passes a Series (row) to the function. `"full"` passes the entire DataFrame. |

## Plugin Registration

This package exposes a standard `data_designer` plugin entry point:

- **Entry Point**: `data_designer.plugins`
- **Name**: `lambda-column`
- **Impl**: `data_designer_lambda_column.plugin.LambdaColumnGenerator`

It will be automatically discovered by `data-designer` when installed in the same environment.
