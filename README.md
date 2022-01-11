# Cooking Recipe ETL

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Spark data pipeline that preprocess and transforms data to provide analytics about cooking recipe difficulty levels.

This Batch system can fulfill following tasks:
1. `preprocess` - Read Json data and simplify structure
2. `transform` - Read preprocessed data and derive insights

## Execution steps
This repo uses `Makefile` for build, test and run.
To know more please run `make help` under root dir.

#### To run preprocess(local) :
```
make execute-local step=preProcess input=input/ output=pre_processed/
```
#### To run transform process(local) :
```
make execute-local step=transform input=pre_processed/ output=output/
```

Make file does contain steps for cluster level execution step.
Build dependency `make build` packed tar and then ship it to cluster.

## Dependency management
Using `poetry` for packaging and managing dependencies within the job.

## Output
```
+----------+----------------------+
|difficulty|avg_total_cooking_time|
+----------+----------------------+
|      EASY|                19.625|
|    MEDIUM|                  45.0|
|      HARD|     194.3913043478261|
+----------+----------------------+
```

## CI
For CI, it is preferable to use github action/workflow management tool.
It would be responsible for building, running test cases and checking lints.

## Airflow as CD
For CD, I have kept airflow dag, which can run spark job on the EMR and terminate cluster
on completion. It can run on scheduled intervals.

## Diagnose and Tuning of application
There can be many possible reason for performance impact, but very first and essential 
step would be to go to spark UI and identify which stage is taking more time. By backtracking and 
syncing with code written for such transformation we can identify the bottleneck.

## Data validation
For data validation, we can run different sets of checks.
1. Data sampling
2. Applying check sum
3. Validate data formatting
4. Data freshness rate
5. Consistency across multiple matrices

### Courtesy
To convert ISO time to second, following stack overflow thread used,
https://stackoverflow.com/questions/67338933/how-to-convert-a-time-value-inside-a-string-from-pt-format-to-seconds.
Thanks to `@mck`.

For project structure, some inspiration was taken from `https://github.com/mehd-io/pyspark-boilerplate-mehdio`