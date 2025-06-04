# MLOps Lab 7

### Config Spark
#### Ресурсы
##### Main
- spark.executor.memory              8g
- spark.driver.memory                4g
- spark.executor.cores               4
##### Datamart
- spark.executor.memory              8g
- spark.driver.memory                6g
- spark.executor.cores               4

#### Параллелизм
- spark.sql.shuffle.partitions       50
- spark.default.parallelism          50

#### Производительность
- spark.serializer                   org.apache.spark.serializer.KryoSerializer
- spark.sql.execution.arrow.pyspark.enabled  true

#### Защита от out-of-memory
- spark.driver.maxResultSize         2g