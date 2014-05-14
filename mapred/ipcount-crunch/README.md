
Crunch based distinct IP counting in weblog data
------------------
Build artefact
	
	$ mvn package
	
Copy artefact to the remote server.

	$ scp target/ipc-crunch-1.0-SNAPSHOT-jar-with-dependencies.jar <user>@<host>:/tmp/


Execute Crunch pipeline.

	$ hadoop jar /tmp/ipc-crunch-1.0-SNAPSHOT-jar-with-dependencies.jar ch.ymc.crunch.DistinctCounter <in> <out>