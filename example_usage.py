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

# 1. Define the output format using Pydantic's BaseModel.
# Using a wrapper object is often more reliable for LLM structured outputs than a raw list.
class GreetingsResponse(BaseModel):
    greetings: list[str]


MODEL_PROVIDER = "nvidia"
MODEL_ID = "nvidia/nemotron-3-nano-30b-a3b"
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

    # 2. Add sample column for input variation
    builder.add_column(
        SamplerColumnConfig(
            name="count",
            sampler_type="category",
            params=CategorySamplerParams(values=[2, 3]),
        )
    )

    # 3. Use LLMStructuredColumnConfig to generate the greetings wrapper.
    builder.add_column(
        LLMStructuredColumnConfig(
            name="greetings_data",
            output_format=GreetingsResponse,
            prompt="Create {{count}} distinct greetings in different languages",
            model_alias=MODEL_ALIAS,
        )
    )

    # 4. Define transformation to extract and explode the list
    def explode_greetings(data, drop_temp=False):
        # data['greetings_data'] contains the GreetingsResponse objects (or dicts).
        # We first extract the specific list field.
        # Note: Depending on data_designer version, this might be a dict or object.
        # We handle both for safety in this example.
        def extract_list(item):
            if hasattr(item, 'greetings'):
                return item.greetings
            if isinstance(item, dict) and 'greetings' in item:
                return item['greetings']
            return []

        # Create a temporary column with just the lists
        data['temp_greetings_list'] = data['greetings_data'].apply(extract_list)
        
        # Explode the list column
        expanded_data = data.explode("temp_greetings_list")
        
        # Assign to the target column name expected by LambdaColumnConfig
        expanded_data["greetings_expanded"] = expanded_data["temp_greetings_list"]

        if drop_temp:
            expanded_data.drop(columns=["temp_greetings_list"], inplace=True)
        
        return expanded_data

    # 5. Apply the transformation
    builder.add_column(
        LambdaColumnConfig(
            name="greetings_expanded",
            # We depend on the generated 'greetings_data' column
            required_cols=["greetings_data"],
            operation_type="full",
            column_function=explode_greetings,
            keyword_arguments={"drop_temp": True}
        )
    )

    # Generate data
    print("Generating data...")
    # Generating fewer records for quicker verification
    results = data_designer.create(builder, num_records=2)
    
    df = results.load_dataset()
    print("\nResulting DataFrame:")
    print(df)
    

if __name__ == "__main__":
    main()
