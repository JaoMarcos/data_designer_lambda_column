from data_designer_lambda_column.plugin import LambdaColumnConfig
from data_designer.essentials import (
    CategorySamplerParams,
    DataDesigner,
    DataDesignerConfigBuilder,
    SamplerColumnConfig,
    LLMStructuredColumnConfig,
    ModelConfig,
    ChatCompletionInferenceParams,
)
from pydantic import BaseModel

class Greatings(BaseModel):
    greetings: list[str]


MODEL_PROVIDER = "nvidia"

# The model ID is from build.nvidia.com.
MODEL_ID = "nvidia/nemotron-3-nano-30b-a3b"

# We choose this alias to be descriptive for our use case.
MODEL_ALIAS = "nemotron-nano-v3"

model_configs = [
    ModelConfig(
        alias=MODEL_ALIAS,
        model=MODEL_ID,
        provider=MODEL_PROVIDER,
        inference_parameters=ChatCompletionInferenceParams(
            temperature=1.0,
            top_p=1.0,
            max_tokens=2048,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}},
        ),
    )
]

def main():
    data_designer = DataDesigner()
    builder = DataDesignerConfigBuilder(model_configs=model_configs)

    # Add a regular column
    builder.add_column(
        SamplerColumnConfig(
            name="a",
            sampler_type="category",
            params=CategorySamplerParams(values=[2, 3, 4]),
        )
    )

    builder.add_column(
    LLMStructuredColumnConfig(
        name="greetings",
        output_format=Greatings,
        prompt="""Create {a} distinct greetings in different languages""",
        model_alias=MODEL_ALIAS,
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
        return list(data["greetings"]["greetings"])

    builder.add_column(
        LambdaColumnConfig(
            name="split_greetings",
            required_cols=["greetings"],
            operation_type="row",
            column_function=split,
            drop=True
        )
    )
    
    def explode_array(data):
        data = data.explode('split_greetings')
        data['greeting'] = data['split_greetings']
        return data

    # explode the array column
    builder.add_column(
        LambdaColumnConfig(
            name="greeting",
            required_cols=["split_greetings"],
            operation_type="full",
            column_function=explode_array
        )
    )

    # Generate data
    results = data_designer.create(builder, num_records=2)
    print(results.load_dataset())

if __name__ == "__main__":
    main()
