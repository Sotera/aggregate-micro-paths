Aggregate Micro Paths
=====================

> An analytic to help infer movement patterns from large amounts of geo-temporal data in a cloud environment.

---

<img src="https://raw.github.com/Sotera/aggregate-micro-paths/master/docs/images/europe-1.png" align="center" width="600" />

## What Do You Provide? ##
A collection of independent entries that represent an identified object's geographic location at a given point in time.

	Key Data Fields [ ID, TIMESTAMP, LATITUDE, LONGITUDE ]

Specific formatting and analytic tool configurations for using your own data set(s) is provided within the **[wiki](https://github.com/Sotera/aggregate-micro-paths/wiki)**.

## What Does This Do?
1. Infers movement patterns based on given geo-temporal data and build tracks (or paths) of movement for each unique object in your collection.
2. Determine spacial and temporal co-occurrence for your objects based off the inferred movement patterns.
3. Produce a graph object where relationships are based off the (configurable) definition of geospatial and temporal co-occurrence.

## What Do You Need To Know? ##
In order to utilize your own data sets, some knowledge of the following aspects will be required:
* **[Apache Hive](http://hive.apache.org/)** syntax
* **[Python programming language](https://www.python.org/)**

## Software Dependencies ##
* **[Cloudera CDH 4.x](http://www.cloudera.com/content/cloudera/en/products-and-services/cdh.html)**, Hadoop {streaming}
* **[Apache Hive](http://hive.apache.org/)**
* **[Python programming language](https://www.python.org/)** + gmpy

### Quick Start

#### Example code

To run the example, execute run_ais.sh found in {project-root}/hive-streaming.  This script will unpack the sample data, upload it to the hadoop filesystem, enter it into Hive, and run the aggregate micro pathing algorithm.  When completed, it will also pull down the finished count data from Hive and place it locally into a .csv file located in the hive-streaming/output directory.

For detailed instructions, [go here](https://github.com/Sotera/aggregate-micro-paths/wiki).
