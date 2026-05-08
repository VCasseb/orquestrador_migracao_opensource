import pandas_gbq
df = pandas_gbq.read_gbq('SELECT * FROM `prj.raw.customers`')
pandas_gbq.to_gbq(df, 'prj.bronze.customers')
