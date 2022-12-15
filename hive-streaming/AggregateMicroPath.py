# Copyright 2016 Sotera Defense Solutions Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

from time import time
import sys
import subprocess
from optparse import OptionParser
import sys

#Add the conf path to our path so we can call the blanketconfig 
from conf.config import AggregateMicroPathConfig

#Differences are the sort order and the table schema for creation
#
# Subprocess wrapper to exit on errors.
#
def subprocessCall(argsList,quitOnError=True,stdout=None):
  returnCode = subprocess.call(argsList,stdout=stdout)
  if quitOnError and returnCode != 0:
    print("Error executing subprocess:\n")
    print(" ".join(argsList))
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
def create_new_hive_table(database_name,table_name,table_schema):
  hql_script = """
    DROP TABLE """+database_name+"""."""+table_name+""";
    CREATE TABLE """+database_name+"""."""+table_name+""" ( """+table_schema+""" )
    ;"""
  subprocessCall(["hive","-e",hql_script]) 


#
# Extract paths from  conf/osm.ini initial data and store into a new table
#
def extract_paths(conf):
  table_schema = "id string, alat string, blat string, alon string, blon string, adt string, bdt string, time string, distance string, velocity string"
  create_new_hive_table(conf.database_name,"micro_path_track_extract_" + conf.table_name,table_schema)



  #hadoop streaming to extract paths
  hql_script = """
    set mapred.reduce.tasks=96;
    set mapred.map.tasks=96;
   
    ADD FILES conf/config.py conf/"""+conf.config_file+""" scripts/extract_path_segments.py;
    FROM(
        SELECT """+conf.table_schema_id+""","""+conf.table_schema_dt+""","""+conf.table_schema_lat+""","""+conf.table_schema_lon+""" 
        FROM """ + conf.database_name + """.""" + conf.table_name + """
        DISTRIBUTE BY """+conf.table_schema_id+"""
        SORT BY """+conf.table_schema_id+""","""+conf.table_schema_dt+""" asc
    ) map_out

    INSERT OVERWRITE TABLE """ + conf.database_name + """.micro_path_track_extract_""" + conf.table_name + """
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
  table_schema = "intersectX string, intersectY string, dt string, velocity float, direction float, track_id string"
  create_new_hive_table(configuration.database_name,"micro_path_tripline_bins_" + configuration.table_name,table_schema)
  
  
  #hadoop streaming to extract paths
  hql_script = """

  
    ADD FILES conf/config.py scripts/tripline_bins.py conf/"""+configuration.config_file+""";

    FROM """ + configuration.database_name + """.micro_path_track_extract_""" + configuration.table_name + """
    INSERT OVERWRITE TABLE """ + configuration.database_name + """.micro_path_tripline_bins_""" + configuration.table_name + """
    
    SELECT TRANSFORM(alat, alon, blat, blon, adt, bdt, velocity, id)
    USING \"python tripline_bins.py """ + configuration.config_file + """ \"
    AS intersectX,intersectY,dt,velocity,direction,track_id
    ;   
    """
  print("***hql_script***")
  print(str(hql_script))
  subprocessCall(["hive","-e",hql_script]) 
  
#
# take values form micro_path_tripline_bins and aggregate the counts
#
def aggregate_intersection_list(configuration):
  table_schema ="x string, y string, ids string, dt string"
  create_new_hive_table(configuration.database_name,"micro_path_intersect_list_" + configuration.table_name,table_schema)

  #hadoop streaming to extract paths
  hql_script = """
    set mapred.map.tasks=96;
    set mapred.reduce.tasks=96;
    
    INSERT OVERWRITE TABLE """ + configuration.database_name + """.micro_path_intersect_list_""" + configuration.table_name + """
    
    SELECT 
      intersectX,intersectY,dt
      STUFF((
        SELECT ', ' + [Name] + ':' + CAST([Value] AS VARCHAR(MAX)) 
        FROM #YourTable 
        WHERE (ID = Results.ID) 
        FOR XML PATH(''),TYPE).value('(./text())[1]','VARCHAR(MAX)')
      ,1,2,'') AS ids
    FROM """ + configuration.database_name + """.micro_path_tripline_bins_""" + configuration.table_name + """
    GROUP BY intersectX,intersectY,dt
    
    SELECT intersectX,intersectY,ids,dt
    FROM """ + configuration.database_name + """.micro_path_tripline_bins_""" + configuration.table_name + """
    GROUP BY intersectX,intersectY,dt
    ;   
    """
  subprocessCall(["hive","-e",hql_script])

#
# take values form micro_path_tripline_bins and aggregate the counts
#
def aggregate_intersection_points(configuration):
  table_schema ="x string, y string, value int, dt string"
  create_new_hive_table(configuration.database_name,"micro_path_intersect_counts_" + configuration.table_name,table_schema)

  #hadoop streaming to extract paths
  hql_script = """
    set mapred.map.tasks=96;
    set mapred.reduce.tasks=96;
    
    INSERT OVERWRITE TABLE """ + configuration.database_name + """.micro_path_intersect_counts_""" + configuration.table_name + """
    SELECT intersectX,intersectY,count(1),dt
    FROM """ + configuration.database_name + """.micro_path_tripline_bins_""" + configuration.table_name + """
    GROUP BY intersectX,intersectY,dt
    ;   
    """
  subprocessCall(["hive","-e",hql_script])
  
def aggregate_intersection_velocity(configuration):
  table_schema ="x string, y string, velocity float, dt string"
  create_new_hive_table(configuration.database_name,"micro_path_intersect_velocity_" + configuration.table_name,table_schema)

  #hadoop streaming to extract paths
  hql_script = """
    set mapred.map.tasks=96;
    set mapred.reduce.tasks=96;
    
    INSERT OVERWRITE TABLE """ + configuration.database_name + """.micro_path_intersect_velocity_""" + configuration.table_name + """
    SELECT intersectX,intersectY,avg(velocity),dt
    FROM """ + configuration.database_name + """.micro_path_tripline_bins_""" + configuration.table_name + """
    GROUP BY intersectX,intersectY,dt
    ;   
    """
  subprocessCall(["hive","-e",hql_script]) 
                    
def aggregate_intersection_direction(configuration):
  table_schema ="x string, y string, direction int, dt string"
  create_new_hive_table(configuration.database_name,"micro_path_intersect_direction_" + configuration.table_name,table_schema)

  #hadoop streaming to extract paths
  hql_script = """
    set mapred.map.tasks=96;
    set mapred.reduce.tasks=96;
    
    INSERT OVERWRITE TABLE """ + configuration.database_name + """.micro_path_intersect_direction_""" + configuration.table_name + """
    SELECT intersectX,intersectY,avg(direction),dt
    FROM """ + configuration.database_name + """.micro_path_tripline_bins_""" + configuration.table_name + """
    GROUP BY intersectX,intersectY,dt
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
  
  # aggregate intersection velocity
  print ("aggregate intersection velocity")
  aggregate_intersection_velocity(configuration)
  
  # aggregate intersection vdirection
  print ("aggregate intersection direction")
  aggregate_intersection_direction(configuration)

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
