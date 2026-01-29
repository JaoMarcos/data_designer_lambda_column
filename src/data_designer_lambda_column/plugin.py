import logging
from typing import Literal, List, Dict
import pandas as pd
from data_designer.config.column_configs import SingleColumnConfig
from data_designer.engine.column_generators.generators.base import ColumnGeneratorFullColumn
from data_designer.plugins import Plugin, PluginType

from pydantic import Field

# Data Designer uses the standard Python logging module for logging
logger = logging.getLogger(__name__)

class ColumnFunction:
    def __init__(self, name: str, func: callable):
        self.name = name
        self.func = func

class LambdaColumnConfig(SingleColumnConfig):
    """Configuration for the lambda column generator."""
    # Optional list of required columns
    required_cols: list[str] = []
    # Required: discriminator field with a unique Literal type
    column_type: Literal["lambda-column"] = "lambda-column"
    # Optional list of functions to apply to the column
    column_function: callable = Field(default=None, exclude=True)
    # Optional operation type
    operation_type: Literal["row","full"] = "row"
    # Optional list of arguments to pass to the function
    arguments: List[str] = []
    # Optional list of keyword arguments to pass to the function
    keyword_arguments: Dict = {}
    
    @property
    def required_columns(self) -> list[str]:
        return self.required_cols

    @property
    def side_effect_columns(self) -> list[str]:
        return []

class LambdaColumnGenerator(ColumnGeneratorFullColumn[LambdaColumnConfig]):
    def generate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Generate the column data.
        
        Args:
            data: The current DataFrame being built
            
        Returns:
            The DataFrame with the new column added
        """
        logger.info(
            f"Generating column {self.config.name} "
            f"with expression {self.config.column_function}"
        )
        
        try:
            if self.config.operation_type == "row":
                data[self.config.name] = data.apply(self.config.column_function, axis=1, args=self.config.arguments, **self.config.keyword_arguments)
            elif self.config.operation_type == "full":
                data = self.config.column_function(data, *self.config.arguments, **self.config.keyword_arguments)

            assert self.config.name in data.columns, f"Column {self.config.name} not found in DataFrame"
        except Exception as e:
            logger.error(f"Error evaluating expression '{self.config.column_function}': {e}")
            raise e
            
        return data

# Plugin instance - this is what gets loaded via entry point
plugin = Plugin(
    impl_qualified_name="data_designer_lambda_column.plugin.LambdaColumnGenerator",
    config_qualified_name="data_designer_lambda_column.plugin.LambdaColumnConfig",
    plugin_type=PluginType.COLUMN_GENERATOR,
    emoji="λ",
)
