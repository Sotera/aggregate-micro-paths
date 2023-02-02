# Test UDF's ability to import other files like config.py.
# Found a solution using pydoc.importfile("filename").
# call_udf.py -> hive -e my_udf.py -> importfile("config.py") -> config.math()

import logging
from pathlib import Path
import subprocess

scripts = " ".join([str(x) for x in Path().glob("**/*.py")])
hql_init = f"""set mapred.reduce.tasks=96; set mapred.map.tasks=96;
    set hive.server2.logging.operation.level=EXECUTION;
    ADD FILES {scripts} ;
    LIST FILES;
    """


def call_udf(udf: str = "my_udf.py") -> None:
    hql_script = f"""{hql_init};
    SELECT 
        TRANSFORM (mmsi, dt, latitude, longitude)
        USING 'python {udf}'
        AS my_id,my_date
    FROM amp_data.micro_path_temp
    LIMIT 20
    ;   
    """
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


if __name__ == "__main__":
    call_udf()
