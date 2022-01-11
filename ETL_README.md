## Framework details

In preprocess,
1. spark reads json files and convert `cooking` and `preparation` time into
   seconds.
2. Applying schema to json data.
3. extracts `year` from published date. Considering recipes count would
   not be higher per month basis.
4. storing derived data a parquet file and `partitioned by year` for optimal performance during reads.

In transform, 
1. spark reads processed parquet files and performs following operations.
2. Deriving total cooking time based on given formula `total_cook_time = cookTime + prepTime`
3. Based on total cooking time profiling recipe's difficulty levels
4. Calculate average cooking time per difficulty level
5. Store data as csv 

## Data Exploration

In above framework, the partitioning strategy decided based on number of recipes published.
Based on below analysis, we can actually store/archive data based on year for optimal performance.

```
+----+--------+
|year|count(*)|
+----+--------+
|2003|       4|
|2007|      94|
|2006|      35|
|2013|      36|
|2004|      12|
|2012|     137|
|2009|     172|
|2005|      44|
|2010|     170|
|2011|     157|
|2008|     181|
+----+--------+
```

There are 47 recipes which contains beef out of 1042.
Attached `Data Xploration` notebook.

## Assumptions

0. Null prep time and cook time considered as zero, those would fall in easy category
1. Selected parquet for preprocess data storage, since it provide higher compression ratio and works better with spark
2. partitioned based on year assuming number of recipes won't be higher at month level

