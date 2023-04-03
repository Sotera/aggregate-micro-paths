import os
import sys
import datetime
import math

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

datetimeFormat = "%Y-%m-%dT%H:%M:%S"
timeBucketSeconds = 60*30 #60 seconds times how many number of minutes
deResoluteFactor = 10 #10 = 1 decimal place in lat/longs  100=2 decimal places, etc
windowLength = 8  #how many windows to use for a subpath.  8 windows = 2 hours
minDistance =1.0  #minimum distance between first and last lats.  this is a hack, 0.5 is pretty good

minDT = "2022-01-02T00:00:00"
maxDT = "2022-01-03T00:00:00"

class PointTime:
    def __init__(self, dt, lat, lon):
        self.dt = dt
        self.lat = float(lat)
        self.lon = float(lon)
    
    def getDT(self):
        return self.dt

    def getLat(self):
        return self.lat
    
    def getLong(self):
        return self.lon
    
    def getLatLong(self):
        return (self.lat, self.lon)
    
    def getData(self):
        return (self.dt, self.lat, self.lon)
    
    def _deResoluteFloat(self, f, p):
        x = float(int(f * p)/p)
        return x

    def getQuantizedLatLong(self, quantizeFactor):
        return (self._deResoluteFloat(self.lat, quantizeFactor), self._deResoluteFloat(self.lon, quantizeFactor))
    
    def getQuantizedLatLongStr(self, quantizeFactor):
        return str(int(self.lat * quantizeFactor)) + ":" + str(int(self.lon * quantizeFactor))
        
class TravellingPointTime:
    def __init__(self, name, datetimeFormat="%Y-%m-%dT%H:%M:%S"):
        self.name = name
        self._rawPointTimes = []
        self._datetimeFormat = datetimeFormat
    
    def sortPointTimes(self):
        self._rawPointTimes=sorted(self._rawPointTimes, key=lambda x: x.getDT())
        
    def getRawPointTimes(self):
        return self._rawPointTimes
    
    def addPointTime(self, pt):
        self._rawPointTimes.extend([pt])
        
    def count(self):
        return len(self._rawPointTimes)
    
    def getPath(self):
        return self._rawPointTimes
        
    def getMinDateTime(self):
        return self._rawPointTimes[0].getDT()
    
    def getMaxDateTime(self):
        return self._rawPointTimes[-1].getDT()
    
    def _interpolatePointFromNeighbors(self, dtTarget, prevPoint, nextPoint):
        #given two pointtimes and a time in between, extrapolate a lat long for that time
        dt_prev = prevPoint.getDT()
        lat_prev = float(prevPoint.getLat())
        lon_prev = float(prevPoint.getLong())
        dt_next = nextPoint.getDT()
        lat_next = float(nextPoint.getLat())
        lon_next = float(nextPoint.getLong())
        dto_prev = datetime.datetime.strptime(dt_prev, self._datetimeFormat)
        dto_next = datetime.datetime.strptime(dt_next, self._datetimeFormat)

        if(dt_prev==dt_next):
            return (lat_prev, lon_prev)

        r = (dtTarget - dto_prev) / (dto_next - dto_prev)

        tlat = ((lat_next-lat_prev) * r) + lat_prev
        tlong = ((lon_next-lon_prev) * r) + lon_prev
        return (tlat, tlong)
    
    def _getBlankPathPointsDict(self, minDT, maxDT, timeBucketSeconds):
        #returns map of all available times, pointtimes are defaulted to none
        pathpoints = {}
        dtX = minDT
        while dtX<=maxDT:
            pathpoints[dtX] = None
            dtX = dtX + datetime.timedelta(0, timeBucketSeconds)   
        return pathpoints
        
    def getQuantizedPath(self, globalMinDT, globalMaxDT, timeBucketSeconds):
        self.sortPointTimes()
        
        minDT = datetime.datetime.strptime(self.getMinDateTime(), self._datetimeFormat)
        maxDT = datetime.datetime.strptime(self.getMaxDateTime(), self._datetimeFormat)

        pathpoints = self._getBlankPathPointsDict(minDT, maxDT, timeBucketSeconds)
        
        aisShipDataLen=len(self._rawPointTimes)
        if(aisShipDataLen<2):
            return pathpoints
        
        dtX = minDT
        idxS = 1 #start with 2nd element, we always need element n-1, so we dont want to take the first element
        
        while dtX<=maxDT:
            currentPathPoint = self._rawPointTimes[idxS]
            currentPathPointDT = datetime.datetime.strptime(currentPathPoint.getDT(), self._datetimeFormat)
            
            while(idxS<aisShipDataLen) and (currentPathPointDT<dtX):
                idxS = idxS + 1
                currentPathPoint = self._rawPointTimes[idxS]
                currentPathPointDT = datetime.datetime.strptime(currentPathPoint.getDT(), self._datetimeFormat)
                
            previousPathPoint = self._rawPointTimes[idxS-1]
            previousPathPointDT = datetime.datetime.strptime(previousPathPoint.getDT(), self._datetimeFormat)
            if(previousPathPointDT<=dtX) and (currentPathPointDT>=dtX):
                (interp_lat, interp_long) = self._interpolatePointFromNeighbors(dtX, previousPathPoint, currentPathPoint)
                pathPoint = PointTime(dtX, interp_lat, interp_long)
                pathpoints[dtX] = pathPoint
            dtX = dtX + datetime.timedelta(0, timeBucketSeconds)

        return pathpoints        
    
    def total_distance(self):
        #just uses the quadratic formula, differences in lat squared, difference of lon squared, sqrrt for a rough estimate
        
        pA = self._rawPointTimes[0]
        pB = self._rawPointTimes[-1]
        (latA, lonA) = pA.getLatLong()
        (latB, lonB) = pB.getLatLong()
        dA = (latB-latA)
        dB = (lonB-lonA)
        tempDistance = math.sqrt((dA*dA) + (dB*dB))
        return tempDistance

def isUsablePath(mmsipath, minPoints=15):
    #is there movement, are there enough valid points.  good way to ignore paths with only 1-2 entries
    validPoints = 0
    firstPoint = None
    lastPoint = None

    for x in mmsipath:
        p = mmsipath[x]
        if(p is not None):
            validPoints = validPoints + 1
            if(firstPoint is None):
                firstPoint = p
            lastPoint = p

    if(firstPoint is not None):
        (latA, lonA) = firstPoint.getLatLong()
    if(lastPoint is not None):
        (latB, lonB) = lastPoint.getLatLong()

    return (firstPoint is not None) and (lastPoint is not None) and (latA!=latB) and (lonA !=lonB) and (validPoints>=minPoints)

def doForEachPartition(rows):
    c=0
    rv = {}

    lastmmsi = None
    tpp = None
    result = []

    for r in rows:
        mmsi = r["mmsi"]

        if(mmsi!=lastmmsi):
            if(tpp is not None):
                qpathPoints = tpp.getQuantizedPath(minDT, maxDT, timeBucketSeconds)
                dis = tpp.total_distance()
                if((dis>minDistance) and isUsablePath(qpathPoints)):
                    mmsiLen = len(qpathPoints)
                    ks = list(qpathPoints.keys())

                    for subpathX in range(mmsiLen-windowLength):
                        subPathStr = []
                        startK = ks[subpathX]
                        firstPathStr = None
                        lastPathStr = None
                        for subpathY in range(windowLength):
                            k = ks[subpathX+subpathY]
                            p = qpathPoints[k].getQuantizedLatLongStr(100)
                            if(firstPathStr is None):
                                firstPathStr = p
                            lastPathStr = p
                            subPathStr.extend([p])
                        if(firstPathStr!=lastPathStr):
                            s = ":".join(subPathStr)   #I needed a unique key to identify a path.
                            hs = hash(s)
                            rw = Row(mmsi = lastmmsi, dt = startK, pathHash = hs, pathStr = s)
                            result.extend([rw])

            lastmmsi = mmsi
            tpp = TravellingPointTime(mmsi)

        bt = r["basedatetime"]
        lat = r["lat"]
        lon = r["lon"]
        pt = PointTime(bt, lat, lon)
        tpp.addPointTime(pt)

    return result

appName= "AISPath_Hashes"
master= "local"
spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()


df = spark.table("ais.ais_raw").filter("basedatetime like '2022-01-02%'")
#df.printSchema()

#minDT = df.select("basedatetime").rdd.min()[0]
#maxDT = df.select("basedatetime").rdd.max()[0]
#minDT = "2022-01-02T00:00:00"
#maxDT = "2022-01-03T00:00:00"
print(minDT, maxDT)


df2=df.repartition(200).repartition("mmsi").sortWithinPartitions("mmsi","basedatetime")

df3 = df2.rdd.mapPartitions(doForEachPartition)

deptColumns = ["mmsi","dt", "pathhash", "pathStr"]
df4 = df3.toDF(deptColumns)
#df4.printSchema()
#df4.show(truncate=False)
df4.write.mode('overwrite').saveAsTable("ais.mmsi_path_hashes")


