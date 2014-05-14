Cascading based distinct IP counting in weblog data
------------------
Build artefact
	
	$ mvn package
	
Copy artefact to the remote server.

	$ scp target/ipc-cascading-1.0-SNAPSHOT-jar-with-dependencies.jar <user>@<host>:/tmp/


Execute Cascading topology.

	$ hadoop jar /tmp/ipc-cascading-1.0-SNAPSHOT-jar-with-dependencies.jar ch.ymc.cascading.Driver <in> <out>