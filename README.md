Aggregate Micro Paths
=====================

<img src="https://raw.github.com/Sotera/aggregate-micro-paths/master/docs/images/europe-1.png" align="right" width="600" />

Infer movement patterns from large amounts of geo-temporal data in a cloud environment.

Visit the [wiki](https://github.com/Sotera/aggregate-micro-paths/wiki) for more information.

## Quick Start

#### Prerequisites

* Hive
* Hadoop (streaming)
* Python + gmpy

#### Example code

To run the example, execute run_ais.sh found in /hive-streaming.  This script will unpack the sample data, upload it to the hadoop filesystem, enter it into Hive, and run the aggregate micro pathing algorithm.  When completed, it will also pull down the finished count data from Hive and place it locally into a .csv file located in the hive-streaming/output directory.

For detailed instructions, go here.
