from pyspark.sql import functions as F

data2.select(F.split('_c0', '; ').alias('_c0'))

col_sizes = data2.select(F.size('_c0').alias('_c0'))
col_max = col_sizes.agg(F.max('_c0'))
columns = col_max.collect()[0][0]

data2.select(*[data2['_c0'][i] for i in range(columns)])
