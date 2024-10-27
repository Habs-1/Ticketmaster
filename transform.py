import pandas as pd

def init_drop(df: pd.DataFrame):

    drop_cols = [
        'images',
        'locale',
        'test'
    ]

    df = df.drop(columns=drop_cols, errors = 'ignore')

    return df

def transform_data(df: pd.DataFrame):
    
    # contains incompatible info for snowflake toSQL function, information does not seem too important besides maybe aliases. 
    # might circle back to fixing these columns at a later date. 
    exclude = [
    '_embedded_venues_markets',
    '_embedded_venues_dmas',
    '_embedded_venues_images',
    '_embedded_venues_aliases',
    '_links_attractions',
    '_embedded_attractions',
    'promoters',
    'products',
    'sales_presales',
    'outlets'
]    
    df = df.drop(columns=exclude, errors='ignore')

    return df

def flatten_nested_json(df, prefix_sep='_'):
    # Initialize an empty DataFrame to store the flattened data
    flattened_df = pd.DataFrame()

    for column in df.columns:
        # If the column has a nested dictionary or list of dictionaries, flatten it
        if isinstance(df[column].iloc[0], dict):
            # Use json_normalize to flatten the nested dictionary
            expanded_df = pd.json_normalize(df[column], sep=prefix_sep)
            expanded_df.columns = [f"{column}{prefix_sep}{subcol}" for subcol in expanded_df.columns]
            flattened_df = pd.concat([flattened_df, expanded_df], axis=1)
        elif isinstance(df[column].iloc[0], list) and isinstance(df[column].iloc[0][0], dict):
            # Flatten lists of dictionaries
            expanded_df = pd.json_normalize(df[column].explode().dropna(), sep=prefix_sep)
            expanded_df.columns = [f"{column}{prefix_sep}{subcol}" for subcol in expanded_df.columns]
            flattened_df = pd.concat([flattened_df, expanded_df], axis=1)
        else:
            # If the column is not nested, add it directly
            flattened_df[column] = df[column]

    return flattened_df