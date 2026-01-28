from data_designer_lambda_column.plugin import LambdaColumnConfig
from data_designer.essentials import (
    CategorySamplerParams,
    DataDesigner,
    DataDesignerConfigBuilder,
    SamplerColumnConfig,
)

def main():
    data_designer = DataDesigner()
    builder = DataDesignerConfigBuilder()

    # Add a regular column
    builder.add_column(
        SamplerColumnConfig(
            name="a",
            sampler_type="category",
            params=CategorySamplerParams(values=[10, 20, 30]),
        )
    )

    # Add a array column
    builder.add_column(
        SamplerColumnConfig(
            name="a_array",
            sampler_type="category",
            params=CategorySamplerParams(values=["abc", "def"]),
        )
    )
    
     # Add a regular column
    builder.add_column(
        SamplerColumnConfig(
            name="b",
            sampler_type="category",
            params=CategorySamplerParams(values=[1, 2, 3]),
        )
    )

    # Add custom lambda column
    # This evaluates 'a + b' for each row (or vectorized)
    builder.add_column(
        LambdaColumnConfig(
            name="sum_ab",
            required_cols=["a", "b"],
            operation_type="row",
            column_function=lambda row: row["a"] + row["b"]
        )
    )

    def split(data):
        return list(data["a_array"])

    builder.add_column(
        LambdaColumnConfig(
            name="split_a_array",
            required_cols=["a_array"],
            operation_type="row",
            column_function=split
        )
    )
    
    def explode_array(data):
        data = data.explode('split_a_array')
        data['explode_a_array'] = data['split_a_array']
        return data

    # explode the array column
    builder.add_column(
        LambdaColumnConfig(
            name="explode_a_array",
            required_cols=["split_a_array"],
            operation_type="full",
            column_function=explode_array
        )
    )

    # Generate data
    results = data_designer.create(builder, num_records=10)
    print(results.load_dataset())

if __name__ == "__main__":
    main()
