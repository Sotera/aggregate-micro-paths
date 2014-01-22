# Prepare data in HDFS and Hive table
hadoop fs -rm /tmp/ais_smallone/*
hadoop fs -rmdir /tmp/ais_smallone
hadoop fs -mkdir /tmp/ais_smallone
gzip -d aisShipData.csv.gz
hadoop fs -put aisShipData.csv /tmp/ais_smallone/
hive -f etl.sql

# Create output directory, remove old output
mkdir -p output
rm -f output/micro_path_ais_results.csv

# Run Job
python AggregateMicroPath.py -c ais.ini

# Get Results
echo -e "latitude\tlongitude\tcount\tdate" > output/micro_path_ais_results.csv
hive -S -e "select * from micro_path_intersect_counts_ais_small_final;" >> output/micro_path_ais_results.csv

