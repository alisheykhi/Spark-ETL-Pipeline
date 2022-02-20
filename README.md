# Spark-ETL-Pipeline
This project is an example for reading data from kafka as a Spark DataFrame and writing objects from Spark into hive database after performing transformation.

## Version Compatibility

Scala| Spark|sbt     
--- | --- | ---
2.11.12| 2.4.0| 1.3.13

## Build from Source
```bash 
$ sbt assembly
```
## Run 

in order to run this application in kerberos enabled environment use the following command. 
you have to create your jaas.config file based on your production configuration.

```bash
spark-submit
 --deploy-mode cluster
 --files "spark_jaas.conf#spark_jaas.conf,your_keytabfile..keytab#your_keytabfile..keytab"
 --driver-java-options "-Djava.security.auth.login.config=./spark_jaas.conf"
 --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./spark_jaas.conf"
 --conf spark.yarn.submit.waitAppCompletion=false
 --driver-memory 16G
 --name SPARK_ETL
 --files config.ini,log4j.properties,spark_jaas.conf,your_keytabfile..keytab
 path_to_your_jar_file.jar path_to_log4j.properties path_to_config.ini

```
