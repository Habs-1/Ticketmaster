import pandas as pd


def transform_data(df: pd.DataFrame):
    
    exclude = ['test','images','locale', 'classifications']

    # columns to fix then re-include: 
    # classifications, 
    


    df.drop(columns=exclude, errors='ignore')