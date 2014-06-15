#/bin/bash
mkdir -p ~/.ivy2/local/org.apache.giraph/giraph-core/1.0.0/jars/
cp lib/giraph-core-1.0.0.jar ~/.ivy2/local/org.apache.giraph/giraph-core/1.0.0/jars/giraph-core.jar
#mvn install:install-file -Dfile=lib/giraph-core-1.0.0.jar -DgroupId=org.apache.giraph -DartifactId=giraph-core -Dversion=1.0 -Dpackaging=jar
