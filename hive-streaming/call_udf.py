import logging
from pathlib import Path
import subprocess

path = Path()  # Path("./scripts")

scripts = " ".join([str(x) for x in path.glob("**/*.py")])
hql_init = f"""set mapred.reduce.tasks=96; set mapred.map.tasks=96;
    set hive.server2.logging.operation.level=EXECUTION;
    ADD FILES {scripts};
    LIST FILES;
    """


def call_udf(udf: str = "my_udf.py") -> None:
    hql_script = f"""{hql_init};
    SELECT 
        TRANSFORM (mmsi, dt, latitude, longitude)
        USING \"python {udf}\"
        AS my_id,my_date
    FROM amp_data.micro_path_temp
    LIMIT 10
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
