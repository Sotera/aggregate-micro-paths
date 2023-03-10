"""UDF sent to worker nodes to extract path segments.

Usage: 
    Sent by AggregateMicroPaths.py as a user-defined function (UDF).

Author:
    Sotera / KeyW / Jacobs (c) 2016 - 2023

Copyright 2016 Sotera Defense Solutions Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import contextlib
import sys
from datetime import datetime
import math
from pathlib import Path
import logging

assert sys.version_info.major >= 3  # For many things.
assert sys.version_info.minor >= 8  # f-string debug relies on this

curdir = Path(__file__).parent
logging.basicConfig(level=logging.INFO)
logging.debug(f"<<<<<< {__file__} >>>>>> {curdir = }.")

# UDFs cannot easily import local modules. But pydoc.importfile can. So.
from pydoc import importfile

config = importfile("config.py")
from config import AggregateMicroPathConfig


def printUsageAndExit(parser):
    parser.print_help()


def wrapDistances(d1, d2):
    """Modify pair of lat or lon coords to correctly calc shortest distance btw them."""
    if d1 < -90 and d2 > 90:
        d2 = d2 - 360
    elif d2 < -90 and d1 > 90:
        d1 = d1 - 360
    return (d1, d2)


def computeDistanceKM(lat1, lon1, lat2, lon2):
    """Computes haversine distance in km from latlon1 to latlon2."""
    R = 6371
    sin, cos, radians = math.sin, math.cos, math.radians
    (lat1, lat2) = wrapDistances(lat1, lat2)
    (lon1, lon2) = wrapDistances(lon1, lon2)
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def parse_stdin():
    """The main function."""

    current_user = prevline = hash_latlon = dt_parse = None

    for i, line in enumerate(sys.stdin):
        (user_id, dt, lat, lon) = parse_line(line)
        # logging.debug(f"{user_id = }; {dt = }; {lat = }; {lon = }")

        dt_parse = parse_date(dt)
        if not dt_parse:
            logging.debug(f"{dt = } failed to parse. Skip line.")
            continue

        # If user_id changes, update prevline etc. and read next line.
        if user_has_changed(current_user, user_id):
            logging.debug(f"User changed from {current_user} to {user_id}. Skip line.")
            current_user = user_id
            prevline = (user_id, dt_parse, lat, lon)
            hash_latlon = {}
            continue

        delta = dt_parse - prevline[1]
        total_time = float(delta.days * 24 * 60 * 60 + delta.seconds)
        # if too much time had passed... then skip the line
        if total_time > configuration.time_filter:
            logging.debug(f"{total_time = } > {configuration.time_filter = }. Skip line.")
            continue

        (auid, adt, alt, aln) = prevline
        (buid, bdt, blt, bln) = (user_id, dt_parse, lat, lon)
        try:
            alt = float(alt)
            aln = float(aln)
            blt = float(blt)
            bln = float(bln)
        except Exception as err:
            logging.error(f"*** EXCEPTION after float() block in ${__file__} ***")
            logging.error(f"*** -> {err=}, {type(err)=} ***")
            continue

        distance = computeDistanceKM(alt, aln, blt, bln)

        # if the distance was too large, skip the segment
        if distance > configuration.distance_filter:
            logging.debug(f"{distance = }km > {configuration.distance_filter = }.")
            continue

        # calculate km / hr   (TODO??? Was there supposed to be a velocity filter here?  -crt)

        latitude_diff = abs(alt - blt)
        longitude_diff = abs(aln - bln)

        if latitude_diff + longitude_diff > 0:
            # We actually went somewhere and didn't stay stationary
            hash_latlon[f"{alt}, {aln}, {blt}, {bln}"] = 1
            segment = [user_id, alt, blt, aln, bln, adt, bdt, total_time, distance, -1]
            if total_time != 0:
                segment[-1] = distance / (total_time / 3600)
            segment = "\t".join(str(x) for x in segment)
            print(segment)  # stdout

        prevline = (user_id, dt_parse, lat, lon)
        # logging.debug(f"{prevline = }")


def parse_line(line: str) -> tuple:
    """Remove quotes, split on tabs, strip"""
    return (x.strip() for x in line.replace('"', "").split("\t"))  # remove quotes
    # (user_id, dt, lat, lon) = line.strip().split("\t")
    # (user_id, dt, lat, lon) = map(lambda x: x.strip(), line.split("\t"))


def parse_date(dt: str) -> datetime:
    """Return parsed date in '2012-03-26 10:18:00[.nnnn]' format.
    (Return None if parse fails.)
    """
    try:
        return dateStrptime(dt.split(".")[0])
    except Exception as err:
        logging.error(f"*** EXCEPTION after dt_parse in ${__file__} ***")
        logging.error(f"*** -> {err=}, {type(err)=} ***")
        return None


def dateStrptime(dt: str) -> datetime:
    """Handle both 'T' and ' ' separators."""
    with contextlib.suppress(ValueError):
        return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")
    return None


def user_has_changed(current_user, user_id):
    return current_user is None or current_user != user_id


#####

if __name__ == "__main__":
    logging.debug(f"{__name__}: {sys.argv = }")
    config_file = sys.argv.pop()  # TODO use argparse or such
    configuration = AggregateMicroPathConfig(config_file)
    parse_stdin()
