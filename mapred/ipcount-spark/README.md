Spark based distinct IP's counting in weblog data.
------------------
Build artefact
	
	$ mvn package

Copy artefact to the remote server.

	$ scp target/ipc-spark-1.0-SNAPSHOT.jar <user@<host>:/tmp/

Set up the classpath.

	$ source /etc/spark/conf/spark-env.sh
	$ export SPARK_CLASSPATH=/tmp/ipc-spark-1.0-SNAPSHOT.jar

Run the Spark application in standalone mode.

	$ $SPARK_HOME/bin/spark-class ch.ymc.spark.DistinctCounter spark://http://<host>:18080 <in> <out>

**Resources**

[Running Spark Applications](http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH5/latest/CDH5-Installation-Guide/cdh5ig_running_spark_apps.html)


