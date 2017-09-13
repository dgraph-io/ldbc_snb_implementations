1. Download SNB dataset (factor 1)[https://www.dropbox.com/s/ajyatjh8s9tnapy/snb_data.tar.gz?dl=0] or (factor 1)[https://www.dropbox.com/s/3ddwh98nojkcx38/snb-003.tar.gz?dl=0], or you can generate new one with (SNB datagen)[https://github.com/ldbc/ldbc_snb_datagen].

2. Convert dataset into `rdf.gz` format with `interactive/dgraph/script/csv_to_rdf.py`, them load output `rdf.gz` file with `dgraphloader`, schema file is at `interactive/dgraph/schema`.

3. Download and install (`ldbc_driver`)[https://github.com/ldbc/ldbc_driver.git] with `mvn install -DskipTest`.

4. Download and install (`dgraph4j`)[https://github.com/dgraph-io/dgraph4j] with `gradle install`.

5. Download and compile bechmark suite from https://github.com/dgraph-io/ldbc_snb_implementations, cd into `interactive/dgraph/run`, modify `dgraph_configuration.properties`, change `queryDir`, `host` and `port` properties to appropriate values, where `queryDir` is the directory contains all queries.

6. Replace `updateStream.properties` with the file with the same name in SNB dataset you got in step 1, original file was copied from pregenerated SNB dataset factor 1.

7. If you need to run some specific queries only, change settings in `ldbc_snb_interactive_SF-[NNNN].properties`.

8. Run `run/run.sh` to start the benchmark, the result is in `run/results`.
