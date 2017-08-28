-----------------------------
Download Dgraph:
-----------------------------
In order to use this benchmark with Dgraph, you have to download and install the
up to date version of Dgraphm.
Follow the document at https://docs.dgraph.io/get-started/

-----------------------------------
Generate SNB dataset:
-----------------------------------
Follow the doc at https://github.com/ldbc/ldbc_snb_datagen to generate test dataset

-----------------------------------
Compile Db class for Dgraph:
-----------------------------------

Go to java/dgraph folder from this checkout, and run:
   $ mvn clean package

--------------------------------------
Running the driver against Dgraph:
--------------------------------------

You have to update the paths in the `run/run.sh` script, and use it for
running the benchmark.  Before that, please update the configuration
files with your options. This process is explained here:
https://github.com/ldbc/ldbc_driver/wiki

