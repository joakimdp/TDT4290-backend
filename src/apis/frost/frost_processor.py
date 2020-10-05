from typing import Dict
import pandas as pd
import apis.processor as processor


class FrostProcessor(processor.Processor):
    def process(self, data: Dict[str, pd.DataFrame]) -> (
        Dict[str, pd.DataFrame]
    ):
        return data
