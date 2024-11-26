import pandas as pd

def line_date_agg(df, variable, time_of_day='14:24:00'):
    df_temp = df.xs(('line', variable), level=('location', 'variable'))
    df_temp = df_temp.reset_index()
    df_temp['date_akdt'] = pd.to_datetime(df_temp['timestamp_akdt'].dt.date) + pd.Timedelta(time_of_day)
    df_temp = pd.merge(df_temp.groupby(['site', 'date_akdt', 'position'])['value'].mean(),
                       df_temp[['site', 'date_akdt', 'timestamp_akdt']].groupby(
                           ['site', 'date_akdt']).mean(),
                       left_index=True, right_index=True)
    df_temp = df_temp.rename(columns={'value':variable, 'timestamp_akdt':'meantime_akdt'})
    return df_temp