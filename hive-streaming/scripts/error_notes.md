# Notes on Runtime Error

## tl;dr
The main issue was that Hive was still using Python2 and I had updated the code to Python3.  
Took me way too long to find that.  This would have made it faster:
* Add `sm03.silverdale.test` to `/etc/hosts` so I could look up the job info directly.
* Check the `stderr` and see that it was failing on an f-string.
* Confirm that python2 caused that. 

## The Error

When running AggregateMicroPath.py, it generated an error in a hive query it generates and runs.

> Execution Error, return code 20001 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask. 
An error occurred while reading or writing to your custom script. It may have crashed with an error

Turns out the error was in the hive call `TRANSFORM ... USING python extract_path_segments.py ais.ini`.
But Hive doesn't show the python error.  See [How-to-troubleshoot-Hive-UDTF](https://community.cloudera.com/t5/Community-Articles/How-to-troubleshoot-Hive-UDTF-functions/ta-p/247559) for commentary & solutions. 

Note: this is a red herring:
> Error: ...: Hive Runtime Error while processing row {"mmsi":"100700154","dt":"2012-03-24 14:35:00","latitude":"37.68461","longitude":"122.2191"}

## The Offending Script
The offending script, ignoring path changes:
```sql
    hive -e "
        set mapred.reduce.tasks=96;
        set mapred.map.tasks=96;
    
        ADD FILES scripts/config.py scripts/ais.ini scripts/extract_path_segments.py;
        FROM(
            SELECT mmsi,dt,latitude,longitude 
            FROM amp_data.ais_small_final
            DISTRIBUTE BY mmsi
            SORT BY mmsi,dt asc
        ) map_out

        INSERT OVERWRITE TABLE amp_data.micro_path_track_extract_ais_small_final
        SELECT TRANSFORM(map_out.mmsi, map_out.dt, map_out.latitude, map_out.longitude)
        USING "python extract_path_segments.py ais.ini"
        AS id,alat,blat,alon,blon,adt,bdt,time,distance,velocity
        ; "
```

There were a number of little things caused by changes to the env and other tweaks, but the biggest is that we updated for Python3 -- which is on staging -- but the nodes were still using only Python2.  So all the Py3 stuff was failing. 

What line in the file they failed on was irrelevant -- a red herring.

## Debug

Broke down the query, determined the `USING` cause was the problem.  Following suggestions in the HOWTO, I **debugged outside of Hive**.  
* Run a modified INSERT OVERWRITE to dump the input table to a tab-separated file.
* Then `cat outfile | python extract_path_segments.py ais.ini`.
* (Modify "outfile" and such to use your actual files and folders!)

After some Import tweaking that ran fine. See next section.

## Relative Import

Running `python extract_pathsegments.py ais.ini` from bash failed for two reasons:
1. There's no stdin to process. Hive provides this from the `TRANSFORM` command. Fixed with `cat <outfile> | python ...`.
2. The relative import that worked in hive failed when calling `extract*py` directly from the commandline!

The second is a known feature of Python: relative imports work for _modules_ that are imported from the main package, but not if the module is run directly as `__main__`.   

There are workarounds like `python -m scripts.extract_pathsegments.py ...` from the parent dir. But they were fragile.

**Solved by PUTTING CONF FILES IN SAME FOLDER!**  No need for relative imports now.  😀👍
