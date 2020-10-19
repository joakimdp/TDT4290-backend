import pandas as pd


def to_csv(df, path):
    """
    Store dataframe as a csv files with datatypes for all columns
    """
    # Prepend dtypes to the top of df
    df2 = df.copy()
    df2.reset_index(inplace=True)
    df2.loc[-1] = df2.dtypes
    df2.index = df2.index + 1
    df2.sort_index(inplace=True)
    # Then save it to a csv
    df2.to_csv(path, index=None)


def read_csv(path):
    """
    Read csv file and parse datatypes
    """
    # Read types first line of csv
    dtypes = {key: value for (key, value) in pd.read_csv(path,
                                                         nrows=1).iloc[0].to_dict().items() if 'date' not in value}

    parse_dates = [key for (key, value) in pd.read_csv(path,
                                                       nrows=1).iloc[0].to_dict().items() if 'date' in value]
    # Read the rest of the lines with the types from above
    return pd.read_csv(path, dtype=dtypes, parse_dates=parse_dates, skiprows=[1])
