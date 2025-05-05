# MLOps Lab 6

### Config Spark
#### Ресурсы
- spark.executor.memory              8g
- spark.driver.memory                4g
- spark.executor.cores               4

#### Параллелизм
- spark.sql.shuffle.partitions       12
- spark.default.parallelism          12

#### Производительность
- spark.serializer                   org.apache.spark.serializer.KryoSerializer
- spark.sql.execution.arrow.pyspark.enabled  true

#### Защита от out-of-memory
- spark.driver.maxResultSize         2g