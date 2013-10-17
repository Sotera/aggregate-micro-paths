from time import time
import sys
import subprocess
from optparse import OptionParser
import sys

#Add the conf path to our path so we can call the blanketconfig 
sys.path.append('conf')
from config import AggregateMicroPathConfig

#Differences are the sort order and the table schema for creation
#
# Subprocess wrapper to exit on errors.
#
def subprocessCall(argsList,quitOnError=True,stdout=None):
  returnCode = subprocess.call(argsList,stdout=stdout)
  if (quitOnError and 0 != returnCode):
    print "Error executing subprocess:\n"
    print " ".join(argsList)
    exit(1)
  return returnCode

#
# print usage to command line and exit
#
def printUsageAndExit(parser):
    parser.print_help()
    exit(1)


#
# create a new hive table
#
def create_new_hive_table(table_name,table_schema):
  hql_script = """
    DROP TABLE """+table_name+""";
    CREATE TABLE """+table_name+""" ( """+table_schema+""" )
    ;"""
  subprocessCall(["hive","-e",hql_script]) 


#
# Extract paths from  conf/osm.ini initial data and store into a new table
#
def extract_paths(conf):
  table_schema = "id string, alat string, blat string, alon string, blon string, adt string, bdt string, time string, distance string, velocity string"
  create_new_hive_table("micro_path_track_extract_" + conf.table_name,table_schema)



  #hadoop streaming to extract paths
  hql_script = """
    set mapred.reduce.tasks=96;
    set mapred.map.tasks=96;
   
    ADD FILES conf/config.py conf/"""+conf.config_file+""" scripts/extract_path_segments.py;
    FROM(
        SELECT """+conf.table_schema_id+""","""+conf.table_schema_dt+""","""+conf.table_schema_lat+""","""+conf.table_schema_lon+""" 
        FROM """ + conf.table_name + """
        DISTRIBUTE BY """+conf.table_schema_id+"""
        SORT BY """+conf.table_schema_id+""","""+conf.table_schema_dt+""" asc
    ) map_out

    INSERT OVERWRITE TABLE micro_path_track_extract_""" + conf.table_name + """
    SELECT TRANSFORM(map_out."""+conf.table_schema_id+""", map_out."""+conf.table_schema_dt+""", map_out."""+conf.table_schema_lat+""", map_out."""+conf.table_schema_lon+""")
    USING \"python extract_path_segments.py """ + conf.config_file + """\"
    AS id,alat,blat,alon,blon,adt,bdt,time,distance,velocity
    ;   
  """
  subprocessCall(["hive","-e",hql_script]) 
  

#
# Extract trip line intersects from paths
#
def extract_trip_line_intersects(configuration):
  table_schema = "intersectX string, intersectY string, dt string"
  create_new_hive_table("micro_path_tripline_bins_" + configuration.table_name,table_schema)
  
  
  #hadoop streaming to extract paths
  hql_script = """

  
    ADD FILES conf/config.py scripts/tripline_bins.py conf/"""+configuration.config_file+""";

    FROM micro_path_track_extract_""" + configuration.table_name + """
    INSERT OVERWRITE TABLE micro_path_tripline_bins_""" + configuration.table_name + """
    
    SELECT TRANSFORM(alat, alon, blat, blon, adt, bdt)
    USING \"python tripline_bins.py """ + configuration.config_file + """ \"
    AS intersectX,intersectY,dt
    ;   
    """
  subprocessCall(["hive","-e",hql_script]) 
  
  

#
# take values form micro_path_tripline_bins and aggregate the counts
#
def aggregate_intersection_points(configuration):
  table_schema ="x string, y string, value int"
  create_new_hive_table("micro_path_intersect_counts_" + configuration.table_name,table_schema)

  #hadoop streaming to extract paths
  hql_script = """
    set mapred.map.tasks=96;
    set mapred.reduce.tasks=96;
    
    INSERT OVERWRITE TABLE micro_path_intersect_counts_""" + configuration.table_name + """
    SELECT intersectX,intersectY,count(*)
    FROM micro_path_tripline_bins_""" + configuration.table_name + """
    GROUP BY intersectX,intersectY
    ;   
    """
  subprocessCall(["hive","-e",hql_script])


#
# 
#
def main(config_file):
 
  start_time = time()
  print('Start time: ' + str(start_time))
  print("Loading config from conf/[{0}]").format(config_file)
  configuration = AggregateMicroPathConfig(config_file, "conf/")
 
 
  print("extracting path data")
  # create a new table and extract path data
  extract_paths(configuration)
  
  # emit points where segemnts intersect with trip line blankets
  print("emit trip line blanket intersects")
  extract_trip_line_intersects(configuration)

  # aggregate intersection points
  print ("aggregate intersection points")
  aggregate_intersection_points(configuration)

  print('End time: ' + str(time() - start_time))


#
# Process command line arguments and run main
#
if __name__ == "__main__":
  parser = OptionParser()
  parser.add_option("-c","--config",
                       dest="configFile",
                       help="REQUIRED: name of configuration file")

  

  (options,args) = parser.parse_args()

  if not options.configFile:
    printUsageAndExit(parser)

  main(options.configFile)
