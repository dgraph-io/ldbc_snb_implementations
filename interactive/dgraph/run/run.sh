#!/bin/bash

cd "`dirname $0`"
PWD=`pwd`

cd ../java/dgraph
mvn package

#cd "$PWD"
cd -

#java -cp /2d1/ldbc/ldbc_driver/target/jeeves-0.3-SNAPSHOT.jar:/2d1/ldbc/ldbc_snb_implementations/interactive/dgraph/java/dgraph/target/dgraph_int-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.ldbc.driver.Client -db com.ldbc.driver.workloads.ldbc.snb.interactive.db.DgraphDb -P /home/ldbc/ldbc_driver/configuration/ldbc/snb/interactive/ldbc_snb_interactive_SF-0030.properties -P /home/ldbc/ldbc_driver/configuration/ldbc_driver_default.properties -P virtuoso_configuration.properties -P /home/sib30-2/snb30data/updates/updateStream.properties -p "ldbc.snb.interactive.parameters_dir|/home/sib30-2/snb30data/substitution_parameters/" -tc 24 -tcr 0.041 -wu 100000 -oc 1000000

CODE_ROOT="$HOME/repos"
DATA_ROOT="/data/social_network"

ls -lF "$PWD/../java/dgraph/target/dgraph_int-0.0.1-SNAPSHOT-jar-with-dependencies.jar"

java -jar "$PWD/../java/dgraph/target/dgraph_int-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
-db com.ldbc.driver.workloads.ldbc.snb.interactive.db.DgraphDb \
-P ldbc_snb_interactive_SF-0001.properties \
-P ldbc_driver_default.properties \
-P dgraph_configuration.properties \
-P updateStream.properties \
-p "ldbc.snb.interactive.parameters_dir|substitution_parameters/" \
-w com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload \
-tc 24 -tcr 0.041 -wu 100000 -oc 1000000
