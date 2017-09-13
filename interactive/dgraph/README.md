Download Dgraph:
-----------------------------
In order to use this benchmark with Dgraph, you have to download and install the
up to date version of Dgraph.
Follow the document at https://docs.dgraph.io/get-started/


Generate SNB dataset:
-----------------------------------
Follow the docs at https://github.com/ldbc/ldbc_snb_datagen to generate test dataset.


Load SNB dataset into dgraph:
-----------------------------------
Use `scripts/csv_to_rdf.py` to convert generated dataset from csv to rdf format. Then use the
Dgraph bulkloader to load it into Dgraph with the schema at scripts/schema.txt


Compile Db class for Dgraph:
-----------------------------------

1. Get an updated dgraph4j library from https://github.com/dgraph-io/dgraph4j
2. Build dgraph4j with:
```
$ gradle clean install
```
3. Go to java/dgraph in this repository and build SNB package with:
```
$ mvn clean package
```


Running the driver against Dgraph:
--------------------------------------

You have to update the paths in the `run/run.sh` script, and use it for running the benchmark.
Before running the script update the following.

1. Change queryDir in run/dgraph_configuration.properties to the absolute path of the queries
   folder.
2. Verify the values of host and port in run/dgraph_configuration.properties.
