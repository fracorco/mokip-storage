# MoKiP Storage Backend
This repository contains the Java code implementing the storage solution of MoKiP, as well as the Java and Python code used for its evaluation. The contents of this repository result from a reorganization and cleanup of the code developed in the ProMo project ("Promo - A Collaborative Agile Approach to Model and Monitor Service-Based Business Processes” was a project funded by the Province of Trento, Italy, within the Operational Programme “Fondo Europeo di Sviluppo Regionale” (FESR) 2007-2013).

## Repository organization
This repository consists of a multi-module Maven project with 3 modules:
* `mokip-storage-backend` - the implementation of MoKiP backend, in Java 8
* `mokip-storage-functions` - optional additional RDF4J SPARQL functions for use with MoKiP (used in ProMo, not in the evaluation)
* `mokip-storage-evaluation` - evaluation code, consisting of a Java synthetic trace simulator, query and population test drivers, plus some python scripts managing the evaluation workflow

## How to compile
The following command compiles all the Java code and generates all the binary artifacts for this project:
```
mvn clean package -DskipTests -Prelease
```
The profile `release` enables the generation of all the artifacts. If omitted, only the JAR files with the binaries for each Maven module are generated.
After running the above command, the compressed archive `mokip-storage-evaluation/target/mokip-storage-evaluation-<VERSION>.tar.gz` contains all the code and input material necessary for the evaluation (see next section).

## How to reproduce the evaluation
Requirements:
* a *test machine* with at least 300GB hard drive (at least 200GB should be SSD). A large amount of RAM is also recommended in order to simulate the largest data sets (up to 500000 traces for 1.5B triples).
* the binaries of *GraphDB standard edition*. This version of GraphDB is not limited in terms of number of accessible CPU cores, however it is not free. You may download it and apply for an evaluation license of 2 months at the following link: https://www.ontotext.com/products/graphdb/graphdb-standard/. This is enough to reproduce the evaluation.
* optionally, to save some computation time, some *precomputed data* available at https://xxxxx

Instructions:
* extract `mokip-storage-evaluation-<VERSION>.tar.gz` on some SSD partition on the test machine (note: sub-folder `data` may be symbolically linked to an HDD location, in order to use only ~200GB of SSD instead of ~300GB, the remaining ~100GB being stored on the HDD);
* extract the GraphDB binaries in the empty folder `software/graphdb` (at the end, you should have an executable `software/graphdb/bin/graphdb` under that folder);
* if precomputed data is available, place all its `.tar.lz` files under folder `data` (these are repository dumps that will reused instead of being regenerated from scratch)
* revise the configuration in `config.py`, if needed (if the requirements are met, no change is needed to this file)
* execute `./test.py` to run the experiments - **this will take several days** (days to weeks to generate each repository, if not available as precomputed data, 6-8 hours for each data size, 7 total) and it will progressively write raw test results under folder `reports`
* execute `./analyze.py` to process the data under `reports` to extract the measures and generate the plots used in the paper, in folder `analysis`
