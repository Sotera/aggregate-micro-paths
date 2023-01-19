# Notes on Runtime Error

## The Error

When running AggregateMicroPath.py, it generated an error in a hive query it generates and runs.

> Execution Error, return code 20001 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask. 
An error occurred while reading or writing to your custom script. It may have crashed with an error

The most relevant error seemed to be:
> Error: ...: Hive Runtime Error while processing row {"mmsi":"100700154","dt":"2012-03-24 14:35:00","latitude":"37.68461","longitude":"122.2191"}

It listed the hive script that caused the error:
```sql
        hive -e 
            set mapred.reduce.tasks=96;
            set mapred.map.tasks=96;
        
            ADD FILES conf/config.py conf/ais.ini scripts/extract_path_segments.py;
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
            ;  
```

That line again:
```json
{"mmsi":"100700154","dt":"2012-03-24 14:35:00","latitude":"37.68461","longitude":"122.2191"}
```

Matches one line in the data. From hive we can find it with:
```sql
hive> select * from micro_path_temp where latitude="37.68461";
--     :                ...Skip 15+ lines of verbosity...
100700154	2012-03-24 14:35:00	37.68461	122.2191
```
That's `mmsi`, `dt`, `latitude`, `longitude`, which are the only four columns in the table. 

In the original CSV we can see it with `grep 37.68461` after fixing newlines:
```bash
$ cat aisShipData.csv | tr "\r" "\n" | grep 37.68461 
ais(3.0), 100700154, 'G C', '', 37.68461, 122.2191, 8.2, 23.0, 1203241435, 'CLASS B', -1
```

The date `dt` comes from the integer `1203241435` in the original CSV, so let's look for near-in-time records.
OK, there's a lot, but here's the first several:
```csv
ais(3.0), 413768739, 'JANG HAI TONG 58', '', 31.14248, 121.9137, 8.9, 310.0, 1203241435, '', -1
ais(3.0), 565352000, 'TAI HUNG SAN', '', 21.76863, 67.48112, 13.6, 147.0, 1203241435, 'SINGAPORE', 1204021900
ais(3.0), 440600000, 'OCEAN DREAM', '', 33.85507, 131.2432, 7.8, 284.0, 1203241435, 'WA_KR POHANG', 1203251700
ais(3.0), 271000264, 'C.TURGAY KALKAVAN', '', 36.43346, 22.91723, 10.7, 260.0, 1203241435, 'VENEZIA\ITALY', 1203281000
```

Nothing obviously wrong with our problem line, compared to these.

## Debug

First I confirmed I could run the subquery inside the `FROM (...)` clause.  No problem.
I saved that output to the table:  `micro_path_temp` to save time. Then we can set env once with:
```sql
      use amp_data;
      set mapred.reduce.tasks=96; set mapred.map.tasks=96;
      ADD FILES conf/config.py conf/ais.ini scripts/extract_path_segments.py;  -- Make avail to the TRANSFORM command.
```
Then this reproduced the error:
```sql
        FROM (SELECT * FROM micro_path_temp) AS map_out
        INSERT OVERWRITE TABLE amp_data.micro_path_track_extract_ais_small_final
        SELECT TRANSFORM(map_out.mmsi, map_out.dt, map_out.latitude, map_out.longitude)
        USING "python extract_path_segments.py ais.ini"
        AS id,alat,blat,alon,blon,adt,bdt,time,distance,velocity
        ;  
```

I had replaced bare "except:" with "except ValueError:" thinking I'd see the error.
But because it runs inside hive, the error is suppressed in the regular output. 
  [x] Replace with `except Exception as err:` and print result.  Still not in stdout.
  [x] Use logging.  Done, but logfile doesn't appear. Need to flush each line?
  [ ] Look for logfiles in cloudera

## Relative Import

The `USING` line shows what python module gets run.  But running `python extract_pathsegments.py ais.ini` from bash fails for two reasons:
1. There's no stdin to process. Hive provides this from the `TRANSFORM` command.
2. The relative import fails unless we do `python -m scripts.extract_pathsegments.py conf/ais.ini` from the parent dir. Relative imports fail if we run modules as `__main__`.
