import pandas as pd


def dataframe_difference(df1: pd.DataFrame, df2: pd.DataFrame, merge_on: list) -> pd.DataFrame:
    """Find rows which are different between two DataFrames."""
    comparison_df = df1.merge(df2,
                              indicator=True,
                              how='outer',
                              on=merge_on,
                              suffixes=('', '_y'))

    # Delete columns with suffix _y
    comparison_df = comparison_df[[
        c for c in comparison_df.columns if not c.endswith('_y')]]

    return comparison_df[comparison_df['_merge'] == 'left_only'].drop('_merge', 1)
