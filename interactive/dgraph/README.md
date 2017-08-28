Download Dgraph:
-----------------------------
In order to use this benchmark with Dgraph, you have to download and install the
up to date version of Dgraphm.
Follow the document at https://docs.dgraph.io/get-started/


Generate SNB dataset:
-----------------------------------
Follow the doc at https://github.com/ldbc/ldbc_snb_datagen to generate test dataset.


Load SNB dataset into dgraph:
-----------------------------------
1. Use `create_schema.sh` to create schema on Dgraph.
2. Use `scripts/csv_to_rdf.py` to convert generated dataset to rdf format, gzip the output file,
then use `dgraphloader` to load them.


Compile Db class for Dgraph:
-----------------------------------

1. Get an updated dgraph4j library from https://github.com/windoze/dgraph4j
2. Build dgraph4j with:
```
$ gradle clean install
```
3. Go to java/dgraph folder, and build SNB package with:
```
$ mvn clean package
```


Running the driver against Dgraph:
--------------------------------------

You have to update the paths in the `run/run.sh` script, and use it for running the benchmark.  Before that, please update the configuration files with your options. This process is explained here:
https://github.com/ldbc/ldbc_driver/wiki
