```
import psycopg2
import pandas as pd
import numpy as np
import pandas.io.sql as psql
from datetime import timedelta



def m11_order_forecast_query(weeks_prior, ref_date):
    conn = psycopg2.connect(dbname='airlift_grocer', user='postgres', host='127.0.0.2',port=5440,password='0QDqfKXMabIkIi1ullph')
    if ref_date != 'current_date':
        ref_date = f"date'{ref_date}'"

    query = f"""
        with a as (select date(date_trunc('day',dispatchtime))  as "date"
        ,date(date_trunc('week',dispatchtime))  as "week"
        ,floor(date_part('day',date_trunc('day',{ref_date})-date_trunc('day',dispatchtime))/7) as week_diff
        ,extract(isodow from dispatchtime) as dow
        ,warehouse_id,date_part('hour', dispatchtime)::int  as "hour",count(distinct id) as "orders"
        from order_details od
        where status in ('delivered','return_flow_complete','returned','all_order_items_lost','items_being_checked_in','with_inventory_team','at_warehouse')
        and date_trunc('day',dispatchtime) >= {ref_date} - interval'{(weeks_prior+1)*7}'day
        and date_trunc('day',dispatchtime) < {ref_date}
        group by 1,2,3,4,5,6
        )
        select *,{ref_date} as ref_date
        --,lag(orders) over (partition by dow,warehouse_id,hour order by week) as lw_orders
        from a
        where week_diff <= {weeks_prior}
    """
    return psql.read_sql(query, conn)


def cap_growth(o, g):
    if ((o < min_orders_cap) & (g > week_growth_cap)):
        g = week_growth_cap
    return g


def m11_get_order_forecast_model(weeks_prior=4, ref_date='current_date', weeks_ahead=1, week_growth_cap=0.6, min_orders_cap=30):
    """

        Input parameters:
        weeks_prior:
        ref_date:
        weeks_ahead:
        Week_growth_cap:
        min_orders_cap:

        Returns:
        A tuple containing:
            In position 0 a forecast of orders at date,hourly level
            in position 1 the original dataframe used in the model.
    """

    df_g = m11_order_forecast_query(weeks_prior, ref_date)
    print(df_g)
    df_g.loc[:, 'lw_orders'] = df_g.sort_values(['warehouse_id', 'hour', 'dow', 'week_diff']).groupby([
        'warehouse_id', 'hour', 'dow'])['orders'].shift(-1)
    print(df_g)
    df_g.loc[:, 'growth'] = (df_g['orders']/df_g['lw_orders']) - 1
    print(df_g)
    df_g.loc[:, 'dow_growth'] = df_g.sort_values(['warehouse_id', 'hour', 'dow', 'week']).groupby(
        ['warehouse_id', 'hour', 'dow'])['growth'].expanding().mean().reset_index(drop=True, level=[0, 1, 2])
    print(df_g)
    df_g['week_growth'] = df_g.sort_values(['warehouse_id', 'hour', 'week']).groupby(
        ['warehouse_id', 'hour'])['growth'].expanding().mean().reset_index(drop=True, level=[0, 1, 2])
    print(df_g)
    df_g['mean_growth'] = df_g[['dow_growth', 'week_growth']].mean(axis=1)
    print(df_g)
    df_g['next_week_diff'] = df_g.sort_values(['warehouse_id', 'hour', 'date']).groupby(
        ['warehouse_id', 'hour'])['week_diff'].shift(-1).fillna(0)
    print(df_g)
    forecast = df_g.loc[df_g['next_week_diff'] == 0, :]
    forecast.loc[:, 'next_date'] = forecast['date'] + timedelta(7)
    forecast.loc[:, 'mean_growth'] = forecast[['orders', 'mean_growth']].apply(
        lambda x: cap_growth(x['orders'], x['mean_growth']), axis=1)

    forecast.loc[:, 'predicted_orders'] = round(
        forecast['orders']*(1+forecast['mean_growth']))
    forecast = forecast.loc[forecast['next_date'] >= forecast['ref_date'], [
        'next_date', 'hour', 'warehouse_id', 'mean_growth', 'predicted_orders']]
    ls = [forecast]
    for i in range(1, weeks_ahead):
        t_df = forecast.copy()
        t_df['next_date'] = t_df['next_date'] + timedelta(7*i)
        t_df.loc[:, 'predicted_orders'] = round(
            t_df['predicted_orders']*((1+t_df['mean_growth'])**i))  # .astype('Int64')
        ls.append(t_df)

    forecast = pd.concat(ls)
    return forecast, df_g


def get_actual_orders(start_date, end_date):


    conn = psycopg2.connect(dbname='airlift_grocer', user='postgres', host='127.0.0.2',port=5440,password='0QDqfKXMabIkIi1ullph')
    query = f"""
            select date(date_trunc('day',dispatchtime))  as "date"
            ,warehouse_id
            ,date_part('hour', dispatchtime)::int  as "hour",count(distinct id) as "orders"
            from order_details od
            where status in ('delivered','return_flow_complete','returned','all_order_items_lost','items_being_checked_in','with_inventory_team','at_warehouse')
            and date_trunc('day',dispatchtime) >= date'{start_date}'
            and date_trunc('day',dispatchtime) <= date'{end_date}'
            and date_trunc('day',dispatchtime) <= current_date
            group by 1,2,3

        """
    return psql.read_sql(query, conn)


def evaluate_model(dg):
    start_date = dg['next_date'].min().strftime('%Y-%m-%d')
    end_date = dg['next_date'].max().strftime('%Y-%m-%d')
    actual_orders = get_actual_orders(start_date, end_date)
    print('actual orders: ')
    print(actual_orders)
    test_df = actual_orders.merge(dg, left_on=['date', 'warehouse_id', 'hour'], right_on=[
                                  'next_date', 'warehouse_id', 'hour'], how='left')
    print('test df')
    print(test_df)

    test_df['squared_difference'] = (
        test_df['predicted_orders']-test_df['orders'])**2
    print('squared_difference')
    print(test_df)
    eval_df = test_df.groupby(['warehouse_id', 'date'])[
        'date', 'squared_difference'].mean()
    print('evalDf')
    print(eval_df)
    eval_df['squared_difference'] = eval_df['squared_difference'].pow(1/2)
    print('return evalDf')
    print(eval_df)
    return eval_df


def m1_order_per_ride_query(weeks_prior, ref_date):
    conn = psycopg2.connect(dbname='airlift_grocer', user='postgres', host='127.0.0.2',port=5440,password='0QDqfKXMabIkIi1ullph')
    if ref_date != 'current_date':
        ref_date = f"date'{ref_date}'"
    query = f"""
        with a as (select date(date_trunc('day',dispatchtime))  as "date"
        ,date(date_trunc('week',dispatchtime))  as "week"
        ,floor(date_part('day',date_trunc('day',{ref_date})-date_trunc('day',dispatchtime))/7) as week_diff
        ,extract(isodow from dispatchtime) as dow
        ,warehouse_id,date_part('hour', dispatchtime)::int  as "hour"
        ,count(distinct id)::numeric/count(distinct rider_id)::numeric as orders_per_rider,avg(commute_time) filter (where commute_time < 90) as commute_time
        from order_details od
        where status in ('delivered','return_flow_complete','returned','all_order_items_lost','items_being_checked_in','with_inventory_team','at_warehouse')
            and date_trunc('day',dispatchtime) >= {ref_date} - interval'{(weeks_prior+1)*7}'day
            and date_trunc('day',dispatchtime) < {ref_date}
        group by 1,2,3,4,5,6
        ),

        b as (select warehouse_id,"hour"
        ,percentile_disc({1/weeks_prior}) within group (order by commute_time) as percentile_commute_time
        from a
        where week_diff <= {weeks_prior}
        group by 1,2
        ),

        c as (
        select b.*,orders_per_rider,row_number() over (partition by b.warehouse_id,b."hour" order by commute_time desc) as rank
        from b
        join (select warehouse_id,hour,dow,commute_time,orders_per_rider from a) a on a.warehouse_id=b.warehouse_id and a.hour = b.hour
        where commute_time <= percentile_commute_time
        )

        select warehouse_id,hour,orders_per_rider from c where rank = 1
    """
    return psql.read_sql(query, conn)


def m1_get_riders_required(safety_buffer=0.3, min_riders=10, order_forecast_model=m11_get_order_forecast_model, order_per_ride_model=m1_order_per_ride_query, configs=None):
    # if configs != None:
    #     for k, v in configs.items():
    #         print(f"setting {k} to {v}")
    #         globals()[str(k)] = v
    orders_df = order_forecast_model(weeks_prior=weeks_prior, weeks_ahead=weeks_ahead,
                                     week_growth_cap=week_growth_cap, min_orders_cap=min_orders_cap, ref_date=ref_date)
    print('1. here')
    print(orders_df)

    orders_df = orders_df[0]
    print('2. here')
    print(orders_df)
    order_pr_ride_df = m1_order_per_ride_query(
        weeks_prior=4, ref_date=ref_date)
    print('3. here')
    print(order_pr_ride_df)

    orders_df = orders_df.merge(
        order_pr_ride_df, on=['hour', 'warehouse_id'], how='left')
    print('4. here')
    print(orders_df)
    orders_df['predicted_riders_required'] = np.ceil(
        (orders_df['predicted_orders']/orders_df['orders_per_rider'])*(1+safety_buffer))
    print('5. here')
    print(orders_df)
    orders_df['predicted_riders_required'] = orders_df['predicted_riders_required'].apply(
        lambda a: max(a, min_riders))
    print('return here')
    print(orders_df)
    return orders_df


def m12_order_forecast_query(weeks_prior, ref_date):
    conn = psycopg2.connect(dbname='airlift_grocer', user='postgres', host='127.0.0.2',port=5440,password='0QDqfKXMabIkIi1ullph')
    if ref_date != 'current_date':
        ref_date = f"date'{ref_date}'"

    query = f"""
        with a as (select date(date_trunc('day',dispatchtime))  as "date"
        ,date(date_trunc('week',dispatchtime))  as "week"
        ,floor(date_part('day',date_trunc('day',{ref_date})-date_trunc('day',dispatchtime))/7) as week_diff
        ,extract(isodow from dispatchtime) as dow
        ,warehouse_id
        ,count(distinct id) as "orders"
        from order_details od
        where status in ('delivered','return_flow_complete','returned','all_order_items_lost','items_being_checked_in','with_inventory_team','at_warehouse')
        and date_trunc('day',dispatchtime) >= {ref_date} - interval'{(weeks_prior+1)*7}'day
        and date_trunc('day',dispatchtime) < {ref_date}
        group by 1,2,3,4,5
        )
        select *,{ref_date} as ref_date
        from a
        where week_diff <= {weeks_prior}
    """
    return psql.read_sql(query, conn)


def m12_hourly_order_proportion_query(ref_date, weeks_prior):
    conn = psycopg2.connect(dbname='airlift_grocer', user='postgres', host='127.0.0.2',port=5440,password='0QDqfKXMabIkIi1ullph')
    if ref_date != 'current_date':
        ref_date = f"date'{ref_date}'"

    query = f"""
        with a as (select
        date(date_trunc('day',dispatchtime))  as "date",
        date(date_trunc('week',dispatchtime))  as "week"
        ,floor(date_part('day',date_trunc('day',{ref_date})-date_trunc('day',dispatchtime))/7) as week_diff
        ,extract(isodow from dispatchtime) as dow
        ,warehouse_id
        ,date_part('hour', dispatchtime)::int  as "hour"
        ,count(distinct id) as "orders"
        from order_details od
        where status in ('delivered','return_flow_complete','returned','all_order_items_lost','items_being_checked_in','with_inventory_team','at_warehouse')
        and date_trunc('day',dispatchtime) >= {ref_date} - interval'{(weeks_prior+1)*7}'day
        and date_trunc('day',dispatchtime) < {ref_date}
        group by 1,2,3,4,5,6
        ),

        b as (select *,orders::numeric/sum(orders) over (partition by date,warehouse_id rows between unbounded preceding and unbounded following) as hour_proportion
        from a
        where week_diff <= 3)

        select {ref_date} as ref_date,warehouse_id,hour,avg(hour_proportion) as hour_proportion
        from b
        group by 1,2,3
        order by 2,3
    """
    return psql.read_sql(query, conn)


def m12_get_order_forecast_model(weeks_prior=4, ref_date='2021-07-23', weeks_ahead=2, week_growth_cap=0.02, min_orders_cap=30):
    """

        Input parameters:
        weeks_prior:
        ref_date:
        weeks_ahead:
        Week_growth_cap:
        min_orders_cap:

        Returns:
        A tuple containing:
            In position 0 a forecast of orders at date,hourly level
            in position 1 the original dataframe used in the model.
    """

    df_g = m12_order_forecast_query(weeks_prior, ref_date)
    df_g.loc[:, 'lw_orders'] = df_g.sort_values(['warehouse_id', 'dow', 'week_diff']).groupby([
        'warehouse_id', 'dow'])['orders'].shift(-1)
    df_g.loc[:, 'growth'] = (df_g['orders']/df_g['lw_orders']) - 1
    df_g['week_growth'] = df_g.sort_values(['warehouse_id', 'week']).groupby(
        ['warehouse_id', 'dow'])['growth'].expanding().mean().reset_index(drop=True, level=[0, 1])
    df_g['next_week_diff'] = df_g.sort_values(['warehouse_id', 'week']).groupby(
        ['warehouse_id', 'week'])['week_diff'].shift(-1).fillna(0)
    forecast = df_g.loc[df_g['next_week_diff'] == 0, :]
    forecast.loc[:, 'next_date'] = forecast['date'] + timedelta(7)
    forecast['mean_growth'] = forecast['week_growth']
    forecast.loc[:, 'predicted_orders'] = round(
        forecast['orders']*(1+forecast['mean_growth']))
    forecast = forecast.loc[forecast['next_date'] >= forecast['ref_date'], [
        'next_date', 'dow', 'warehouse_id', 'mean_growth', 'predicted_orders']]
    ls = [forecast]
    for i in range(1, weeks_ahead):
        t_df = forecast.copy()
        t_df['next_date'] = t_df['next_date'] + timedelta(7*i)
        t_df.loc[:, 'predicted_orders'] = round(
            t_df['predicted_orders']*((1+t_df['mean_growth'])**i))  # .astype('Int64')
        ls.append(t_df)

    forecast = pd.concat(ls)
    hr = m12_hourly_order_proportion_query(
        ref_date=ref_date, weeks_prior=weeks_prior)
    forecast = forecast.merge(hr, on='warehouse_id')
    forecast['predicted_orders'] = round(
        forecast['predicted_orders']*forecast['hour_proportion'])
    forecast = forecast[['next_date', 'warehouse_id',
                         'hour', 'mean_growth', 'predicted_orders']]
    return forecast, df_g


weeks_prior = 4
weeks_ahead = 2
week_growth_cap = 0.02,
min_orders_cap = 5,
ref_date = '2021-07-15'
configs = None


dft = m1_get_riders_required(
    order_forecast_model=m12_get_order_forecast_model, configs=configs)

df = evaluate_model(dft)
df
```

    C:\Users\Hassan\anaconda3\lib\site-packages\pandas\core\indexing.py:845: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      self.obj[key] = _infer_fill_value(value)
    C:\Users\Hassan\anaconda3\lib\site-packages\pandas\core\indexing.py:966: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      self.obj[item] = s
    <ipython-input-7-5f6af9f7eced>:293: SettingWithCopyWarning: 
    A value is trying to be set on a copy of a slice from a DataFrame.
    Try using .loc[row_indexer,col_indexer] = value instead
    
    See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
      forecast['mean_growth'] = forecast['week_growth']
    

    1. here
    (       next_date  warehouse_id  hour  mean_growth  predicted_orders
    0     2021-07-15             1     0    -0.025465              61.0
    1     2021-07-15             1     1    -0.025465              40.0
    2     2021-07-15             1     2    -0.025465              25.0
    3     2021-07-15             1     3    -0.025465              19.0
    4     2021-07-15             1     4    -0.025465               9.0
    ...          ...           ...   ...          ...               ...
    5429  2021-07-28           247    19          NaN               NaN
    5430  2021-07-28           247    20          NaN               NaN
    5431  2021-07-28           247    21          NaN               NaN
    5432  2021-07-28           247    22          NaN               NaN
    5433  2021-07-28           247    23          NaN               NaN
    
    [5434 rows x 5 columns],            date        week  week_diff  dow  warehouse_id  orders    ref_date  \
    0    2021-06-11  2021-06-07        4.0  5.0             1     689  2021-07-15   
    1    2021-06-11  2021-06-07        4.0  5.0            35     511  2021-07-15   
    2    2021-06-11  2021-06-07        4.0  5.0            37     751  2021-07-15   
    3    2021-06-11  2021-06-07        4.0  5.0            38     842  2021-07-15   
    4    2021-06-11  2021-06-07        4.0  5.0            39     721  2021-07-15   
    ..          ...         ...        ...  ...           ...     ...         ...   
    611  2021-07-14  2021-07-12        0.0  3.0           242     203  2021-07-15   
    612  2021-07-14  2021-07-12        0.0  3.0           243     226  2021-07-15   
    613  2021-07-14  2021-07-12        0.0  3.0           245     182  2021-07-15   
    614  2021-07-14  2021-07-12        0.0  3.0           246      58  2021-07-15   
    615  2021-07-14  2021-07-12        0.0  3.0           247     500  2021-07-15   
    
         lw_orders    growth  week_growth  next_week_diff  
    0          NaN       NaN          NaN             4.0  
    1          NaN       NaN          NaN             4.0  
    2          NaN       NaN          NaN             4.0  
    3          NaN       NaN          NaN             4.0  
    4          NaN       NaN          NaN             4.0  
    ..         ...       ...          ...             ...  
    611      297.0 -0.316498    -0.182532             0.0  
    612      329.0 -0.313070     0.510184             0.0  
    613      121.0  0.504132     1.074790             0.0  
    614       49.0  0.183673     0.657054             0.0  
    615        NaN       NaN          NaN             0.0  
    
    [616 rows x 11 columns])
    2. here
           next_date  warehouse_id  hour  mean_growth  predicted_orders
    0     2021-07-15             1     0    -0.025465              61.0
    1     2021-07-15             1     1    -0.025465              40.0
    2     2021-07-15             1     2    -0.025465              25.0
    3     2021-07-15             1     3    -0.025465              19.0
    4     2021-07-15             1     4    -0.025465               9.0
    ...          ...           ...   ...          ...               ...
    5429  2021-07-28           247    19          NaN               NaN
    5430  2021-07-28           247    20          NaN               NaN
    5431  2021-07-28           247    21          NaN               NaN
    5432  2021-07-28           247    22          NaN               NaN
    5433  2021-07-28           247    23          NaN               NaN
    
    [5434 rows x 5 columns]
    3. here
         warehouse_id  hour  orders_per_rider
    0               1     0          2.851852
    1               1     1          2.055556
    2               1     2          1.105263
    3               1     3          1.500000
    4               1     4          1.142857
    ..            ...   ...               ...
    387           247    19          1.054054
    388           247    20          1.161290
    389           247    21          1.040000
    390           247    22          1.000000
    391           247    23          1.071429
    
    [392 rows x 3 columns]
    4. here
           next_date  warehouse_id  hour  mean_growth  predicted_orders  \
    0     2021-07-15             1     0    -0.025465              61.0   
    1     2021-07-15             1     1    -0.025465              40.0   
    2     2021-07-15             1     2    -0.025465              25.0   
    3     2021-07-15             1     3    -0.025465              19.0   
    4     2021-07-15             1     4    -0.025465               9.0   
    ...          ...           ...   ...          ...               ...   
    5429  2021-07-28           247    19          NaN               NaN   
    5430  2021-07-28           247    20          NaN               NaN   
    5431  2021-07-28           247    21          NaN               NaN   
    5432  2021-07-28           247    22          NaN               NaN   
    5433  2021-07-28           247    23          NaN               NaN   
    
          orders_per_rider  
    0             2.851852  
    1             2.055556  
    2             1.105263  
    3             1.500000  
    4             1.142857  
    ...                ...  
    5429          1.054054  
    5430          1.161290  
    5431          1.040000  
    5432          1.000000  
    5433          1.071429  
    
    [5434 rows x 6 columns]
    5. here
           next_date  warehouse_id  hour  mean_growth  predicted_orders  \
    0     2021-07-15             1     0    -0.025465              61.0   
    1     2021-07-15             1     1    -0.025465              40.0   
    2     2021-07-15             1     2    -0.025465              25.0   
    3     2021-07-15             1     3    -0.025465              19.0   
    4     2021-07-15             1     4    -0.025465               9.0   
    ...          ...           ...   ...          ...               ...   
    5429  2021-07-28           247    19          NaN               NaN   
    5430  2021-07-28           247    20          NaN               NaN   
    5431  2021-07-28           247    21          NaN               NaN   
    5432  2021-07-28           247    22          NaN               NaN   
    5433  2021-07-28           247    23          NaN               NaN   
    
          orders_per_rider  predicted_riders_required  
    0             2.851852                       28.0  
    1             2.055556                       26.0  
    2             1.105263                       30.0  
    3             1.500000                       17.0  
    4             1.142857                       11.0  
    ...                ...                        ...  
    5429          1.054054                        NaN  
    5430          1.161290                        NaN  
    5431          1.040000                        NaN  
    5432          1.000000                        NaN  
    5433          1.071429                        NaN  
    
    [5434 rows x 7 columns]
    return here
           next_date  warehouse_id  hour  mean_growth  predicted_orders  \
    0     2021-07-15             1     0    -0.025465              61.0   
    1     2021-07-15             1     1    -0.025465              40.0   
    2     2021-07-15             1     2    -0.025465              25.0   
    3     2021-07-15             1     3    -0.025465              19.0   
    4     2021-07-15             1     4    -0.025465               9.0   
    ...          ...           ...   ...          ...               ...   
    5429  2021-07-28           247    19          NaN               NaN   
    5430  2021-07-28           247    20          NaN               NaN   
    5431  2021-07-28           247    21          NaN               NaN   
    5432  2021-07-28           247    22          NaN               NaN   
    5433  2021-07-28           247    23          NaN               NaN   
    
          orders_per_rider  predicted_riders_required  
    0             2.851852                       28.0  
    1             2.055556                       26.0  
    2             1.105263                       30.0  
    3             1.500000                       17.0  
    4             1.142857                       11.0  
    ...                ...                        ...  
    5429          1.054054                        NaN  
    5430          1.161290                        NaN  
    5431          1.040000                        NaN  
    5432          1.000000                        NaN  
    5433          1.071429                        NaN  
    
    [5434 rows x 7 columns]
    actual orders: 
                date  warehouse_id  hour  orders
    0     2021-07-15             1     0      30
    1     2021-07-15             1     1      23
    2     2021-07-15             1     2      16
    3     2021-07-15             1     3       5
    4     2021-07-15             1     4       1
    ...          ...           ...   ...     ...
    4819  2021-07-28           281    19       7
    4820  2021-07-28           281    20       6
    4821  2021-07-28           281    21       8
    4822  2021-07-28           281    22       6
    4823  2021-07-28           281    23       8
    
    [4824 rows x 4 columns]
    test df
                date  warehouse_id  hour  orders   next_date  mean_growth  \
    0     2021-07-15             1     0      30  2021-07-15    -0.025465   
    1     2021-07-15             1     1      23  2021-07-15    -0.025465   
    2     2021-07-15             1     2      16  2021-07-15    -0.025465   
    3     2021-07-15             1     3       5  2021-07-15    -0.025465   
    4     2021-07-15             1     4       1  2021-07-15    -0.025465   
    ...          ...           ...   ...     ...         ...          ...   
    4819  2021-07-28           281    19       7         NaN          NaN   
    4820  2021-07-28           281    20       6         NaN          NaN   
    4821  2021-07-28           281    21       8         NaN          NaN   
    4822  2021-07-28           281    22       6         NaN          NaN   
    4823  2021-07-28           281    23       8         NaN          NaN   
    
          predicted_orders  orders_per_rider  predicted_riders_required  
    0                 61.0          2.851852                       28.0  
    1                 40.0          2.055556                       26.0  
    2                 25.0          1.105263                       30.0  
    3                 19.0          1.500000                       17.0  
    4                  9.0          1.142857                       11.0  
    ...                ...               ...                        ...  
    4819               NaN               NaN                        NaN  
    4820               NaN               NaN                        NaN  
    4821               NaN               NaN                        NaN  
    4822               NaN               NaN                        NaN  
    4823               NaN               NaN                        NaN  
    
    [4824 rows x 9 columns]
    squared_difference
                date  warehouse_id  hour  orders   next_date  mean_growth  \
    0     2021-07-15             1     0      30  2021-07-15    -0.025465   
    1     2021-07-15             1     1      23  2021-07-15    -0.025465   
    2     2021-07-15             1     2      16  2021-07-15    -0.025465   
    3     2021-07-15             1     3       5  2021-07-15    -0.025465   
    4     2021-07-15             1     4       1  2021-07-15    -0.025465   
    ...          ...           ...   ...     ...         ...          ...   
    4819  2021-07-28           281    19       7         NaN          NaN   
    4820  2021-07-28           281    20       6         NaN          NaN   
    4821  2021-07-28           281    21       8         NaN          NaN   
    4822  2021-07-28           281    22       6         NaN          NaN   
    4823  2021-07-28           281    23       8         NaN          NaN   
    
          predicted_orders  orders_per_rider  predicted_riders_required  \
    0                 61.0          2.851852                       28.0   
    1                 40.0          2.055556                       26.0   
    2                 25.0          1.105263                       30.0   
    3                 19.0          1.500000                       17.0   
    4                  9.0          1.142857                       11.0   
    ...                ...               ...                        ...   
    4819               NaN               NaN                        NaN   
    4820               NaN               NaN                        NaN   
    4821               NaN               NaN                        NaN   
    4822               NaN               NaN                        NaN   
    4823               NaN               NaN                        NaN   
    
          squared_difference  
    0                  961.0  
    1                  289.0  
    2                   81.0  
    3                  196.0  
    4                   64.0  
    ...                  ...  
    4819                 NaN  
    4820                 NaN  
    4821                 NaN  
    4822                 NaN  
    4823                 NaN  
    
    [4824 rows x 10 columns]
    evalDf
                             squared_difference
    warehouse_id date                          
    1            2021-07-15          314.958333
                 2021-07-16          119.958333
                 2021-07-17          348.333333
                 2021-07-18          265.583333
                 2021-07-19          231.625000
    ...                                     ...
    281          2021-07-24                 NaN
                 2021-07-25                 NaN
                 2021-07-26                 NaN
                 2021-07-27                 NaN
                 2021-07-28                 NaN
    
    [286 rows x 1 columns]
    return evalDf
                             squared_difference
    warehouse_id date                          
    1            2021-07-15           17.747065
                 2021-07-16           10.952549
                 2021-07-17           18.663690
                 2021-07-18           16.296728
                 2021-07-19           15.219231
    ...                                     ...
    281          2021-07-24                 NaN
                 2021-07-25                 NaN
                 2021-07-26                 NaN
                 2021-07-27                 NaN
                 2021-07-28                 NaN
    
    [286 rows x 1 columns]
    

    <ipython-input-7-5f6af9f7eced>:129: FutureWarning: Indexing with multiple keys (implicitly converted to a tuple of keys) will be deprecated, use a list instead.
      eval_df = test_df.groupby(['warehouse_id', 'date'])[
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th></th>
      <th>squared_difference</th>
    </tr>
    <tr>
      <th>warehouse_id</th>
      <th>date</th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="5" valign="top">1</th>
      <th>2021-07-15</th>
      <td>17.747065</td>
    </tr>
    <tr>
      <th>2021-07-16</th>
      <td>10.952549</td>
    </tr>
    <tr>
      <th>2021-07-17</th>
      <td>18.663690</td>
    </tr>
    <tr>
      <th>2021-07-18</th>
      <td>16.296728</td>
    </tr>
    <tr>
      <th>2021-07-19</th>
      <td>15.219231</td>
    </tr>
    <tr>
      <th>...</th>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th rowspan="5" valign="top">281</th>
      <th>2021-07-24</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2021-07-25</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2021-07-26</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2021-07-27</th>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2021-07-28</th>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>286 rows × 1 columns</p>
</div>


