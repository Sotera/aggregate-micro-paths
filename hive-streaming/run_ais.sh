# Copyright 2016 Sotera Defense Solutions Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License”);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

database=$(sed -n 's/.*database_name *: *\([^ ]*.*\)/\1/p' < ./conf/ais.ini)

echo "Using '${database}' for database."

# Prepare data in HDFS and Hive table
hadoop fs -rm -f /tmp/ais_smallone/*
hadoop fs -rmdir -f /tmp/ais_smallone
hadoop fs -mkdir /tmp/ais_smallone
[ -e aisShipData.csv.gz ] && gzip -d aisShipData.csv.gz
hadoop fs -put aisShipData.csv /tmp/ais_smallone/
hive --hiveconf database=${database} -f etl.sql

# Create output directory, remove old output
mkdir -p output
rm -f output/micro_path_ais_results.csv


# Run Job
python AggregateMicroPath.py -c ais.ini

# Get Results
echo -e "latitude\tlongitude\tcount\tdate" > output/micro_path_ais_results.csv
hive -S -e "select * from ${database}.micro_path_intersect_counts_ais_small_final;" >> output/micro_path_ais_results.csv
