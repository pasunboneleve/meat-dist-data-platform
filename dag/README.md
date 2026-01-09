DAGs
====

This is where the DAGs live. The dependencies are just so we sanely
know what we're building for in Cloud Composer/Airflow, provide type
linting in the IDE, and know when we need to add dependencies in the Cloud.


Spark Runtime
-------------

Get it from
[Maven](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5_2.13/1.10.1). Click
on the [jar
file](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.13/1.10.1/iceberg-spark-runtime-3.5_2.13-1.10.1.jar)
and place it in the `DEPS_BUCKET`.


```sh
gcloud storage cp \
   ~/Downloads/iceberg-spark-runtime-3.5_2.13-1.10.1.jar \
   gs://$PROJECT_ID-deps-bucket
```
