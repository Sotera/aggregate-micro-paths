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
import sys
import subprocess
from optparse import OptionParser
import logging
from pathlib import Path

assert sys.version_info.major >= 3  # For many things.
assert sys.version_info.minor >= 8  # f-string debug relies on this

logging.basicConfig(level=logging.WARNING)

# We run on staging so do a regular import.
# (The UDF functions have to use a different method.)
from conf.config import AggregateMicroPathConfig as AMP_Config


def main(config_file, base_dir="."):

    print(f'Start time: {strftime("%d %b %Y %H:%M:%S", localtime())}')
    start_time = time()  # Higher precision

    print(f"Loading config from {base_dir}/{config_file}")
    configuration = AMP_Config(config_file, base_dir)
    set_globals(configuration)

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


def set_globals(C: AMP_Config) -> None:
    """Declare global vars for boilerplate hql script."""
    global hql_init
    scripts = " ".join([str(x) for x in Path().glob("**/*.py")])
    hql_init = f"""set mapred.reduce.tasks=96; set mapred.map.tasks=96;
    set hive.server2.logging.operation.level=EXECUTION;
    ADD FILES {scripts} {C.config_file};
    """
    # LIST FILES; -- if you need for debugging


def extract_paths(
    C: AMP_Config,  # Global shared config
    pyscript: str = "extract_path_segments.py",  # UDF for this query
) -> None:
    """Extract paths from  conf/osm.ini initial data and store into a new table"""

    out_table: str = "micro_path_track_extract"
    table_schema = (
        "id string, alat string, blat string, alon string, blon string, "
        "adt string, bdt string, time string, distance string, velocity string"
    )
    hql_script = f"""{hql_init};

    FROM(
        SELECT {C.table_schema_id}, {C.table_schema_dt}, {C.table_schema_lat}, {C.table_schema_lon}
        FROM {C.database_name}.{C.table_name}
        DISTRIBUTE BY {C.table_schema_id}
        SORT BY {C.table_schema_id}, {C.table_schema_dt} asc
    ) map_out

    INSERT OVERWRITE TABLE {C.database_name}.{out_table}_{C.table_name}
    SELECT TRANSFORM ( 
        map_out.{C.table_schema_id}, map_out.{C.table_schema_dt}, 
        map_out.{C.table_schema_lat}, map_out.{C.table_schema_lon} )
    USING \"python {pyscript} {C.config_file}\"
    AS id,alat,blat,alon,blon,adt,bdt,time,distance,velocity
    ;   
    """
    query_hive(C, out_table, table_schema, hql_script)


def extract_trip_line_intersects(
    C: AMP_Config,  # Global shared config
    pyscript: str = "tripline_bins.py",  # UDF for this query
) -> None:
    """Extract trip line intersects from paths"""

    in_table = "micro_path_track_extract"
    out_table = "micro_path_tripline_bins"
    table_schema = (
        "intersectX string, intersectY string, dt string, velocity float, "
        "direction float, track_id string"
    )
    hql_script = f"""{hql_init}; 

    FROM {C.database_name}.{in_table}_{C.table_name}
    INSERT OVERWRITE TABLE {C.database_name}.{out_table}_{C.table_name}
    SELECT TRANSFORM ( alat, alon, blat, blon, adt, bdt, velocity, id )
    USING \"python {pyscript} {C.config_file}\"
    AS intersectX,intersectY,dt,velocity,direction,track_id
    ;   
    """
    query_hive(C, out_table, table_schema, hql_script)


def aggregate_intersection_list(
    C: AMP_Config,  # Global shared config
) -> None:
    """Take values form micro_path_tripline_bins and aggregate the counts
    TODO: What the heck is "  # YourTable" in the query below???
    """
    in_table = "micro_path_tripline_bins"
    out_table = "micro_path_intersect_list"
    table_schema = "x string, y string, ids string, dt string"

    hql_script = f"""{hql_init}; 

    INSERT OVERWRITE TABLE {C.database_name}.{out_table}_{C.table_name}
    SELECT 
      intersectX,intersectY,dt
      STUFF((
        SELECT ', ' + [Name] + ':' + CAST([Value] AS VARCHAR(MAX)) 
        FROM #YourTable 
        WHERE (ID = Results.ID) 
        FOR XML PATH(''),TYPE).value('(./text())[1]','VARCHAR(MAX)')
      ,1,2,'') AS ids
    FROM {C.database_name}.{in_table}_{C.table_name}
    GROUP BY intersectX,intersectY,dt
    
    SELECT intersectX,intersectY,ids,dt
    FROM {C.database_name}.{in_table}_{C.table_name}
    GROUP BY intersectX,intersectY,dt
    ;   
    """
    query_hive(C, out_table, table_schema, hql_script)


def aggregate_intersection_points(
    C: AMP_Config,  # Global shared config
) -> None:
    """take values from micro_path_tripline_bins and aggregate the counts"""

    table_schema = "x string, y string, countOf int, dt string"
    in_table = "micro_path_tripline_bins"
    out_table = "micro_path_intersect_counts"
    hql_script = f"""{hql_init}; 
    
    INSERT OVERWRITE TABLE {C.database_name}.{out_table}_{C.table_name}
    SELECT intersectX,intersectY,count(1),dt
    FROM {C.database_name}.{in_table}_{C.table_name}
    GROUP BY intersectX,intersectY,dt
    ;   
    """
    query_hive(C, out_table, table_schema, hql_script)


def aggregate_intersection_velocity(
    C: AMP_Config,  # Global shared config
) -> None:
    """Calculate intersection velocities"""

    in_table = "micro_path_tripline_bins"
    out_table = "micro_path_intersect_velocity"
    table_schema = "x string, y string, velocity float, dt string"
    hql_script = f"""
    set mapred.map.tasks=96; set mapred.reduce.tasks=96;
    
    INSERT OVERWRITE TABLE {C.database_name}.{out_table}_{C.table_name}
    SELECT intersectX,intersectY,avg(velocity),dt
    FROM {C.database_name}.{in_table}_{C.table_name}
    GROUP BY intersectX,intersectY,dt
    ;   
    """
    query_hive(C, out_table, table_schema, hql_script)


def aggregate_intersection_direction(
    C: AMP_Config,  # Global shared config
) -> None:

    in_table = "micro_path_tripline_bins"
    out_table = "micro_path_intersect_direction"
    table_schema = "x string, y string, direction int, dt string"
    hql_script = f"""{hql_init};
    
    INSERT OVERWRITE TABLE {C.database_name}.{out_table}_{C.table_name}
    SELECT intersectX,intersectY,avg(direction),dt
    FROM {C.database_name}.{in_table}_{C.table_name}
    GROUP BY intersectX,intersectY,dt
    ;   
    """
    query_hive(C, out_table, table_schema, hql_script)


def query_hive(
    conf: AMP_Config,  # Global shared config
    prefix: str,  # Table prefix specific to this query
    table_schema: str,  # Schema for our new table
    hql_script: str,  # The script to run
) -> None:
    """Create table {conf.database}.{prefix}_{conf.table_name} matching {table_schema}
    using {hive_query} and UDF {pyscript}."""

    if table_schema:
        create_new_hive_table(conf.database_name, f"{prefix}_{conf.table_name}", table_schema)
    print(f"***hql_script***\n{hql_script}")
    run_and_log_hive(hql_script)


def create_new_hive_table(database_name: str, table_name: str, table_schema: str) -> None:

    hql_script = f"""DROP TABLE {database_name}.{table_name}; 
    CREATE TABLE {database_name}.{table_name} ( {table_schema} )
    ;"""
    run_and_log_hive(hql_script)


def run_and_log_hive(hql_script: str) -> None:
    try:
        subprocessCall(["hive", "-e", hql_script])
    except FileNotFoundError as err:
        logging.error("[run_and_log_hive]: Could not run hive. Check hive is installed & in PATH.")
        exit(1)  # Tidier than raise.  Loses trace info though.


def subprocessCall(argsList, quitOnError=True, stdout=None):
    """Call subprocess and optionally quit on errors"""
    returnCode = subprocess.call(argsList, stdout=stdout)
    if quitOnError and returnCode != 0:
        logging.error(f"Error in subprocess:\n\t{' '.join(argsList)}")
        exit(1)
    return returnCode


def printUsageAndExit(parser):
    parser.print_help()
    exit(1)


def get_opts() -> tuple:
    """Process cmdline args. Return (options, args)."""
    parser = OptionParser()
    parser.add_option(
        "-c", "--config", dest="configFile", help="REQUIRED: name of configuration file"
    )
    options, args = parser.parse_args()
    if not options.configFile:
        printUsageAndExit(parser)
    return options, args


#
#
#
if __name__ == "__main__":
    options, args = get_opts()
    main(options.configFile)
