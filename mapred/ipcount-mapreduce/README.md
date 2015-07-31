Mapreduce based distinct IP's counting in weblog data.
------------------
Build artefact
	
	$ mvn package
	
Copy artefact to the remote server.

	$ scp target/ipc-mapreduce-1.0-SNAPSHOT.jar <user>@<host>:/tmp/


Execute Crunch pipeline.

	$ hadoop jar /tmp/ipc-mapreduce-1.0-SNAPSHOT.jar example.DistinctCounter <in> <out>
