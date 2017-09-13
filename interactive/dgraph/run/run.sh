#!/bin/bash

set -e

cd "`dirname $0`"
PWD=`pwd`

cd ../java/dgraph
mvn package

cd -

ls -lF "$PWD/../java/dgraph/target/dgraph_int-0.0.1-SNAPSHOT-jar-with-dependencies.jar"

java -jar "$PWD/../java/dgraph/target/dgraph_int-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
-db com.ldbc.driver.workloads.ldbc.snb.interactive.db.DgraphDb \
-P ldbc_snb_interactive_SF-0001.properties \
-P ldbc_driver_default.properties \
-P dgraph_configuration.properties \
-P updateStream.properties \
-p "ldbc.snb.interactive.parameters_dir|substitution_parameters/" \
-w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload \
-wu 0 -oc 10
