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

from dataclasses import dataclass
import datetime
import gmpy2  # multi-precision arithmetic
import logging
import math
import numpy as np
from pathlib import Path
import sys

assert sys.version_info.major >= 3  # For many things.
assert sys.version_info.minor >= 8  # f-string debug relies on this

curdir = Path(__file__).parent
logging.basicConfig(level=logging.DEBUG)
logging.debug(f"<<<<<< {__file__} >>>>>> {curdir = }.")

# UDFs cannot easily import local modules. But pydoc.importfile can. So.
from pydoc import importfile

config = importfile("config.py")
from config import AggregateMicroPathConfig


@dataclass
class Point:
    x: float
    y: float


def ccw(A, B, C):
    return -((C.y - A.y) * (B.x - A.x)) + ((B.y - A.y) * (C.x - A.x))


def isgtzero(a):
    return a > 0


def wrapDistances(d1, d2) -> tuple:
    """Wrap a pair of latitudes or longitudes to correctly calc shortest dist.

    Returns modified pair.
    """

    if d1 < -90 and d2 > 90:
        return (d1, d2 - 360)
    if d2 < -90 and d1 > 90:
        return (d1 - 360, d2)
    return (d1, d2)


def computeDistanceKM(lat1, lon1, lat2, lon2) -> float:
    """Compute the distance (in km) between two points in lat / lon.

    Uses the haversine formula.
    """

    (lat1, lat2) = wrapDistances(lat1, lat2)
    (lon1, lon2) = wrapDistances(lon1, lon2)
    R = 6371  # km
    dlat = math.radians(float(lat2) - float(lat1))
    dlon = math.radians(float(lon2) - float(lon1))
    a = float(
        math.sin(dlat / 2) * math.sin(dlat / 2)
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2)
        * math.sin(dlon / 2)
    )
    c = float(2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))
    return float(R * c)


def betweenpts(A1, A2, Q, threshold=0.0000001):
    compAxMin = gmpy2.mpfr(min(A1.x, A2.x) - gmpy2.mpfr(threshold))
    compAxMax = gmpy2.mpfr(max(A1.x, A2.x) + gmpy2.mpfr(threshold))
    compAyMin = gmpy2.mpfr(min(A1.y, A2.y) - gmpy2.mpfr(threshold))
    compAyMax = gmpy2.mpfr(max(A1.y, A2.y) + gmpy2.mpfr(threshold))
    return compAxMin <= Q.x <= compAxMax and compAyMin <= Q.y <= compAyMax


def intersect_gmpy(A, B, C, D):
    acd = ccw(A, C, D)
    bcd = ccw(B, C, D)
    abc = ccw(A, B, C)
    abd = ccw(A, B, D)

    # literal edge cases, when one of our points lies on the opposite line
    if (
        (acd == 0 and betweenpts(A, C, D))
        or (bcd == 0 and betweenpts(B, C, D))
        or (abc == 0 and betweenpts(A, B, C))
        or (abd == 0 and betweenpts(A, B, D))
        or (isgtzero(acd) != isgtzero(bcd) and isgtzero(abc) != isgtzero(abd))
    ):

        denom = gmpy2.mpfr(((D.y - C.y) * (B.x - A.x)) - ((D.x - C.x) * (B.y - A.y)))
        uanumerator = gmpy2.mpfr(((D.x - C.x) * (A.y - C.y)) - ((D.y - C.y) * (A.x - C.x)))
        ubnumerator = gmpy2.mpfr(((B.x - A.x) * (A.y - C.y)) - ((B.y - A.y) * (A.x - C.x)))
        if denom == 0:
            # Lines are parallel, so return no
            return (0, 0, 0)
        else:
            ua = gmpy2.mpfr(uanumerator / denom)
            ub = gmpy2.mpfr(ubnumerator / denom)

            # if ua and ub are both between 0 and 1, then the intersection is in  both segments
            # NOTE: it does not matter which determinant we use for the equations below
            x = gmpy2.mpfr(A.x + (ua * (B.x - A.x)))
            y = gmpy2.mpfr(A.y + (ua * (B.y - A.y)))
            sign = 1

            # if the segment has the same y values (horizontal) then if going west it is negative
            # if the segment has different y values, going south is negative
            if (A.y == B.y and A.x > B.x) or (A.y > B.y):
                sign = sign * -1

            if min(A.x, B.x) <= x <= max(A.x, B.x) and min(A.y, B.y) <= y <= max(A.y, B.y):
                return (x, y, sign)
            else:
                return (0, 0, 0)

    return (0, 0, 0)


def perp(a):
    b = np.empty_like(a)
    b[0] = -a[1]
    b[1] = a[0]
    return b


def seg_intersect(a1, a2, b1, b2):
    da = a2 - a1
    db = b2 - b1
    dp = a1 - b1
    dap = perp(da)
    denom = np.dot(dap, db)
    if denom == 0:
        return (0, 0)
    num = np.dot(dap, dp)
    (intx, inty) = (num / denom) * db + b1
    # Make sure we are within the bounds of the segment
    if (
        min(a1[0], a2[0]) <= intx <= max(a1[0], a2[0])
        and min(b1[0], b2[0]) <= intx <= max(b1[0], b2[0])
        and min(a1[1], a2[1]) <= inty <= max(a1[1], a2[1])
        and min(b1[1], b2[1]) <= inty <= max(b1[1], b2[1])
    ):
        return (intx, inty)
    return (0, 0)


# Returns the point of intersection as the first two parameters (latitude, longitude) and the last parameter as the direction of crossing.
# first two points are the GMTI segment. Second two points are the tripline we check against.
def intersect(a1, a2, b1, b2):
    swap = 0
    if a1[0] == a2[0]:
        (a1, a2, b1, b2, swap) = (b1, b2, a1, a2, 1)
    if a1[0] == a2[0]:
        return (0, 0, 0)
    if b1[0] == b2[0]:
        slopea = (a2[1] - a1[1]) / (a2[0] - a1[0])
        inty = a1[1] + (b1[0] - a1[0]) * slopea
        if min(a1[0], a2[0]) <= b1[0] <= max(a1[0], a2[0]) and min(b1[1], b2[1]) <= inty <= max(
            b1[1], b2[1]
        ):
            if swap and b2[1] > b1[1] or not swap and a2[0] > a1[0]:
                return (b1[0], inty, 1)
            else:
                return (b1[0], inty, -1)
        return (0, 0, 0)
    slopea = (a2[1] - a1[1]) / (a2[0] - a1[0])
    slopeb = (b2[1] - b1[1]) / (b2[0] - b1[0])
    if slopea == slopeb:
        return (0, 0, 0)
    inta = a1[1] - a1[0] * slopea
    intb = b1[1] - b1[0] * slopeb

    intx = (intb - inta) / (slopea - slopeb)
    inty = slopea * intx + inta

    sign = 1
    if slopeb > slopea:
        sign *= -1
    if a1[0] > a2[0]:
        sign *= -1

    if (
        min(a1[0], a2[0]) <= intx <= max(a1[0], a2[0])
        and min(b1[0], b2[0]) <= intx <= max(b1[0], b2[0])
        and min(a1[1], a2[1]) <= inty <= max(a1[1], a2[1])
        and min(b1[1], b2[1]) <= inty <= max(b1[1], b2[1])
    ):
        return (intx, inty, sign)
    else:
        return (0, 0, 0)
    # revisit this line
    # return (intx,inty,-40)


# starttime = time()


def bearing(lat1, lon1, lat2, lon2):
    """Convert a two-point line to a bearing.
    lat1, lon1 = origin
    lat2, lon2 = destination
    """

    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    rlon1 = math.radians(lon1)
    rlon2 = math.radians(lon2)
    dlon = math.radians(lon2 - lon1)

    b = math.atan2(
        math.sin(dlon) * math.cos(rlat2),
        math.cos(rlat1) * math.sin(rlat2) - math.sin(rlat1) * math.cos(rlat2) * math.cos(dlon),
    )  # bearing calc
    bd = math.degrees(b)
    br, bn = divmod(bd + 360, 360)  # the bearing remainder and final bearing

    return bn


def interpolatedTime(start_dt, start_lat, start_lon, end_lat, end_lon, vel):
    distance = computeDistanceKM(start_lat, start_lon, end_lat, end_lon)
    hours = distance / vel if vel > 0.00001 else 0.00001
    seconds = hours * 60 * 60
    return start_dt + datetime.timedelta(seconds=int(round(seconds)))


def temporalSplit(dt, which):
    identity = lambda d: d

    def min10(d):
        """Return d rounded down to nearest 10 minutes."""
        newMin = d.minute - (d.minute % 10)
        return d.replace(minute=newMin, second=0, microsecond=0)

    conds = {
        "all": lambda d: d.replace(
            year=datetime.MAXYEAR, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        ),
        "year": lambda d: d.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0),
        "month": lambda d: d.replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        "day": lambda d: d.replace(hour=0, minute=0, second=0, microsecond=0),
        "hour": lambda d: d.replace(minute=0, second=0, microsecond=0),
        "10min": min10,
        "minute": lambda d: d.replace(second=0, microsecond=0),
    }

    finalDt = conds.get(which.lower(), identity)(dt)
    return finalDt.strftime("%Y-%m-%d %H:%M:%S")


configuration = AggregateMicroPathConfig(sys.argv.pop())
for line in sys.stdin:
    track_row = line.split("\t")
    logging.debug(track_row)
    # (lat1, lon1, lat2, lon2, date1, date2, vel, track_id) = track_row
    id, lat1, lat2, lon1, lon2, date1, date2, _time, _distance, vel = track_row

    track_id = track_id.strip()
    vel = float(vel)
    lat1 = float(lat1)
    lon1 = float(lon1)
    lat2 = float(lat2)
    lon2 = float(lon2)
    direction = bearing(lat1, lon1, lat2, lon2)
    try:
        start_dt = datetime.datetime.strptime(date1, "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        logging.error(e)
        continue

    for blanket in configuration.triplineBlankets:

        tripLat1 = blanket[0]  # 0 lower left
        tripLon1 = blanket[1]  # 1 lower left
        tripLat2 = blanket[2]  # 2 upper right
        tripLon2 = blanket[3]  # 3 upper right
        resolutionLat = blanket[5]
        resolutionLon = blanket[6]

        roundfactorLat = -1 * int(round(math.log(resolutionLat)))
        roundfactorLon = -1 * int(round(math.log(resolutionLon)))

        tlon1 = lon1
        tlon2 = lon2

        A = Point(lat1, lon1)
        B = Point(lat2, lon2)
        # Make sure the blanket covers this segment
        lowerLeftAOI = Point(tripLat1, tripLon1)
        upperRightAOI = Point(tripLat2, tripLon2)
        firstPointOK = betweenpts(lowerLeftAOI, upperRightAOI, A)
        secondPointOK = betweenpts(lowerLeftAOI, upperRightAOI, B)

        if not (firstPointOK or secondPointOK):
            # This will however exclude segments that go over the entire blanket region
            continue

        # check to see if we should route the segment over the international dateline
        if abs(lon1 - lon2) > 180:
            if lon1 > lon2:
                lon1 = lon1 - 360
            else:
                lon2 = lon2 - 360
        # Start iterating over the latitudes (horizontal triplines)
        # these two calls give us the max and min tripline indexes
        latscaledmin = int(math.floor((min(lat1, lat2) - resolutionLat) / resolutionLat))
        latscaledmax = int(math.ceil((max(lat1, lat2) + resolutionLat) / resolutionLat))

        # the max and min calls make sure we don't get fuzzy edges, by clamping the range to the bounding box
        for interval in range(max(latscaledmin, blanket[7]), min(latscaledmax, blanket[8])):
            currentTripLat = float(interval) * resolutionLat
            C = Point(currentTripLat, tripLon1)
            D = Point(currentTripLat, tripLon2)
            # multiply to get the latitude from the interval!
            # (intersectX, intersectY, intersectDir) = intersect((lat1,lon1), (lat2,lon2),(currentTripLat,tripLon1),(currentTripLat,tripLon2))
            # (intersectX, intersectY) = seg_intersect(numpy.array([lat1,lon1]), numpy.array([lat2,lon2]),numpy.array([currentTripLat,tripLon1]),numpy.array([currentTripLat,tripLon2]))
            (intersectX, intersectY, intersectDir) = intersect_gmpy(A, B, C, D)
            # if abs(intersectX - intersectX2)> float(0.0001) or abs(intersectY - intersectY2) > float(0.0001):
            #  print('DIFF')
            #  print(lat1, lon1, lat2, lon2)
            #  print(intersectX, intersectX2, intersectY, intersectY2)
            #  print(currentTripLat, tripLon1, currentTripLat, tripLon2)
            #  out = [lat1,tlon1,lat2,tlon2,date1,date2,intersectX,intersectY]
            #  out = map(lambda x: str(x),out)
            #  print "\t".join(out)

            if intersectX == 0 and intersectY == 0:
                # intersection is not on line segment... off to side
                continue

            # TODO: currently this rounding is hardcoded - need to configure it
            intersectX = round(intersectX + resolutionLat * 0.5, roundfactorLat)
            intersectY = round(
                (intersectY) - (intersectY % resolutionLon) + (resolutionLon * 0.5), roundfactorLon
            )

            # Re-adjust for the international date line
            if intersectY < -180:
                intersectY = intersectY + 360.0
            dt = interpolatedTime(start_dt, lat1, lon1, intersectX, intersectY, vel)
            finalDate = temporalSplit(dt, configuration.temporal_split)
            out = [intersectX, intersectY, finalDate, vel, direction, track_id]
            out = map(lambda x: str(x), out)
            print("\t".join(out))

        # Start iterating over the longitudes (vertical triplines)
        # these two calls give us the max and min tripline indexes
        lonscaledmin = int(math.floor((min(lon1, lon2) - resolutionLon) / resolutionLon))
        lonscaledmax = int(math.ceil((max(lon1, lon2) + resolutionLon) / resolutionLon))

        # the max and min calls make sure we don't get fuzzy edges, by clamping the range to the bounding box
        for interval in range(max(lonscaledmin, blanket[9]), min(lonscaledmax, blanket[10])):
            # multiply to get the longitude from the interval!
            currentTripLon = float(interval) * resolutionLon
            # (intersectX, intersectY, intersectDir) = intersect((lat1,lon1), (lat2,lon2),(tripLat1,currentTripLon),(tripLat2,currentTripLon))
            # (intersectX, intersectY) = seg_intersect(numpy.array([lat1,lon1]), numpy.array([lat2,lon2]),numpy.array([tripLat1,currentTripLon]),numpy.array([tripLat2,currentTripLon]))
            C = Point(tripLat1, currentTripLon)
            D = Point(tripLat2, currentTripLon)
            (intersectX, intersectY, intersectDir) = intersect_gmpy(A, B, C, D)
            # if abs(intersectX - intersectX2)> float(0.0001) or abs(intersectY - intersectY2) > float(0.0001):
            #  print('Diff')
            #  print(lat1, lon1, lat2, lon2)
            #  print(intersectX, intersectX2, intersectY, intersectY2)
            #  print(tripLat1, currentTripLon, tripLat2, currentTripLon)
            #  out = [lat1,tlon1,lat2,tlon2,date1,date2,intersectX,intersectY]
            #  out = map(lambda x: str(x),out)
            #  print "\t".join(out)

            if intersectX == 0 and intersectY == 0:
                # intersection is not on line segment... off to side
                continue

            intersectY = round(intersectY + resolutionLon * 0.5, roundfactorLon)
            intersectX = round(
                (intersectX) - (intersectX % resolutionLat) + (resolutionLat * 0.5), roundfactorLat
            )

            # Re-adjust for the international date line
            if intersectY < -180:
                intersectY = intersectY + 360.0

            dt = interpolatedTime(start_dt, lat1, lon1, intersectX, intersectY, vel)
            finalDate = temporalSplit(dt, configuration.temporal_split)
            out = [intersectX, intersectY, finalDate, vel, direction, track_id]
            out = map(lambda x: str(x), out)
            print("\t".join(out))
# stoptime = time()-starttime
# print(stoptime)
