def transform_confirmed(df):
    df = df.withColumnRenamed("Country/Region", "Country")
    return df

def transform_recovered(df):
    df = df.withColumnRenamed("Province/State", "State")
    return df

def transform_deaths(df):
    df = df.withColumnRenamed("Country/Region", "Region")
    return df
