# Copyright 2016-2023 Sotera Defense Solutions Inc. and Jacobs Inc.
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

from time import time, localtime, strftime
import subprocess
from optparse import OptionParser
import logging

logging.basicConfig(level=logging.WARNING)

# Add the conf path to our path so we can call the blanketconfig
from scripts.config import AggregateMicroPathConfig as AMP_Config


def subprocessCall(argsList, quitOnError=True, stdout=None):
    """Call subprocess and optionally quit on errors"""
    returnCode = subprocess.call(argsList, stdout=stdout)
    if quitOnError and returnCode != 0:
        logging.error("Error in subprocess:\n\t{' '.join(argsList)}")
        exit(1)
    return returnCode


def printUsageAndExit(parser):
    parser.print_help()
    exit(1)


def create_new_hive_table(database_name: str, table_name: str, table_schema: str) -> None:
    hql_script = f"""DROP TABLE {database_name}.{table_name}; 
    CREATE TABLE {database_name}.{table_name} ( {table_schema} )
    ;"""
    subprocessCall(["hive", "-e", hql_script])

def query_hive(
    conf: AMP_Config,    # Global shared config
    prefix: str,         # Table prefix specific to this query
    table_schema: str,   # Schema for our new table
    hql_script: str      # The script to run
    ) -> None:
    """Create table {conf.database}.{prefix}_{conf.table_name} matching {table_schema} 
    using {hive_query} and UDF {pyscript}."""
    create_new_hive_table(
        conf.database_name,
        f"{prefix}_{conf.table_name}",
        table_schema
    )
    print(f"***hql_script***\n{hql_script}")
    subprocessCall(["hive", "-e", hql_script])



  #hadoop streaming to extract paths
  hql_script = """
    set mapred.reduce.tasks=96;
    set mapred.map.tasks=96;
   
    ADD FILES scripts/config.py conf/"""+conf.config_file+""" scripts/extract_path_segments.py;
    FROM(
        SELECT {C.table_schema_id}, {C.table_schema_dt}, {C.table_schema_lat}, {C.table_schema_lon}
        FROM {C.database_name}.{C.table_name}
        DISTRIBUTE BY {C.table_schema_id}
        SORT BY {C.table_schema_id}, {C.table_schema_dt} asc
    ) map_out

    INSERT OVERWRITE TABLE """ + conf.database_name + """.micro_path_track_extract_""" + conf.table_name + """
    SELECT TRANSFORM(map_out."""+conf.table_schema_id+""", map_out."""+conf.table_schema_dt+""", map_out."""+conf.table_schema_lat+""", map_out."""+conf.table_schema_lon+""")
    USING \"python extract_path_segments.py """ + conf.config_file + """\"
    AS id,alat,blat,alon,blon,adt,bdt,time,distance,velocity
    ;   
    """
    query_hive(C, prefix, table_schema, hql_script)


#
# Extract trip line intersects from paths
#
def extract_trip_line_intersects(configuration):
  table_schema = "intersectX string, intersectY string, dt string, velocity float, direction float, track_id string"
  create_new_hive_table(
      configuration.database_name,
      f"micro_path_tripline_bins_{configuration.table_name}",
      table_schema,
  )


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
  print(hql_script)
  subprocessCall(["hive","-e",hql_script]) 
  
#
# take values form micro_path_tripline_bins and aggregate the counts
#
def aggregate_intersection_list(configuration):
  table_schema ="x string, y string, ids string, dt string"
  create_new_hive_table(
      configuration.database_name,
      f"micro_path_intersect_list_{configuration.table_name}",
      table_schema,
  )

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
    FROM {C.database_name}.micro_path_tripline_bins_{C.table_name}
    GROUP BY intersectX,intersectY,dt
    
    SELECT intersectX,intersectY,ids,dt
    FROM {C.database_name}.micro_path_tripline_bins_{C.table_name}
    GROUP BY intersectX,intersectY,dt
    ;   
    """
    )
    query_hive(C, prefix, table_schema, hql_script)


#
# take values form micro_path_tripline_bins and aggregate the counts
#
def aggregate_intersection_points(configuration):
    table_schema = "x string, y string, value int, dt string"
    create_new_hive_table(
        configuration.database_name,
        f"micro_path_intersect_counts_{configuration.table_name}",
        table_schema,
    )

  #hadoop streaming to extract paths
  hql_script = """
    set mapred.map.tasks=96;
    set mapred.reduce.tasks=96;
    
    INSERT OVERWRITE TABLE """
        + configuration.database_name
        + """.micro_path_intersect_counts_"""
        + configuration.table_name
        + """
    SELECT intersectX,intersectY,count(1),dt
    FROM """
        + configuration.database_name
        + """.micro_path_tripline_bins_"""
        + configuration.table_name
        + """
    GROUP BY intersectX,intersectY,dt
    ;   
    """
    )
    subprocessCall(["hive", "-e", hql_script])


def aggregate_intersection_velocity(configuration):
    table_schema = "x string, y string, velocity float, dt string"
    create_new_hive_table(
        configuration.database_name,
        f"micro_path_intersect_velocity_{configuration.table_name}",
        table_schema,
    )

  #hadoop streaming to extract paths
  hql_script = """
    set mapred.map.tasks=96;
    set mapred.reduce.tasks=96;
    
    INSERT OVERWRITE TABLE """
        + configuration.database_name
        + """.micro_path_intersect_velocity_"""
        + configuration.table_name
        + """
    SELECT intersectX,intersectY,avg(velocity),dt
    FROM """
        + configuration.database_name
        + """.micro_path_tripline_bins_"""
        + configuration.table_name
        + """
    GROUP BY intersectX,intersectY,dt
    ;   
    """
    )
    subprocessCall(["hive", "-e", hql_script])


def aggregate_intersection_direction(configuration):
    table_schema = "x string, y string, direction int, dt string"
    create_new_hive_table(
        configuration.database_name,
        f"micro_path_intersect_direction_{configuration.table_name}",
        table_schema,
    )

  #hadoop streaming to extract paths
  hql_script = """
    set mapred.map.tasks=96;
    set mapred.reduce.tasks=96;
    
    INSERT OVERWRITE TABLE """
        + configuration.database_name
        + """.micro_path_intersect_direction_"""
        + configuration.table_name
        + """
    SELECT intersectX,intersectY,avg(direction),dt
    FROM """
        + configuration.database_name
        + """.micro_path_tripline_bins_"""
        + configuration.table_name
        + """
    GROUP BY intersectX,intersectY,dt
    ;   
    """
    )
    subprocessCall(["hive", "-e", hql_script])


#
#
#
def main(config_file, base_dir="./"):

    print(f'Start time: {strftime("%d %b %Y %H:%M:%S", localtime())}')
    start_time = time()  # Higher precision
    print(f"Loading config from {base_dir}/{config_file}")
    configuration = AMP_Config(config_file, base_dir)

    print("extracting path data....................")
    # create a new table and extract path data
    extract_paths(configuration)

    # emit points where segemnts intersect with trip line blankets
    print("emit trip line blanket intersects....................")
    extract_trip_line_intersects(configuration)

    # aggregate intersection points
    print("aggregate intersection points....................")
    aggregate_intersection_points(configuration)

    # aggregate intersection velocity
    print("aggregate intersection velocity....................")
    aggregate_intersection_velocity(configuration)

    # aggregate intersection vdirection
    print("aggregate intersection direction....................")
    aggregate_intersection_direction(configuration)

    Δt_s = time() - start_time
    print(f"Elapsed time: {Δt_s:.3f}s")


#
# Process command line arguments and run main
#
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option(
        "-c", "--config", dest="configFile", help="REQUIRED: name of configuration file"
    )

    (options, args) = parser.parse_args()

    if not options.configFile:
        printUsageAndExit(parser)

    main(options.configFile)
