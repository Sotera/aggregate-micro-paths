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

# Define names
database=$(sed -n 's/.*database_name *: *\([^ ]*.*\)/\1/p' < ./conf/ais.ini)
out_dir=output
output=${out_dir}/micro_path_ais_results.csv
counts_table=micro_path_intersect_counts_ais_small_final

echo "Using '${database}' for database."

echo "Prepare data in HDFS...."
hadoop fs -rm -f /tmp/ais_smallone/*
hadoop fs -rmdir /tmp/ais_smallone
hadoop fs -mkdir /tmp/ais_smallone
[ -e aisShipData.csv.gz ] && [ -z aisShipData.csv ] && gzip -d aisShipData.csv.gz
hadoop fs -put aisShipData.csv /tmp/ais_smallone/

echo "Create HIVE table in ${database}..."
hive --hiveconf database=${database} -f etl.sql

echo "Create output directory, remove old output."
mkdir -p ${out_dir}
rm -f ${output}


echo "**** Run Job ****"
python AggregateMicroPath.py -c ais.ini

echo "**** Get Results ****"
echo -e "latitude\tlongitude\tcount\tdate" > ${output}
hive -S -e "select * from ${database}.${counts_table};" >> ${output}
