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

import contextlib
import sys
import datetime
from datetime import datetime
import math
#sys.path.append('./') 
from ..conf.config import AggregateMicroPathConfig

current_user = None
prevtime = None
prevline = None
hash_latlon = None
dt_parse = None

#
# print usage to command line and exit
#
def printUsageAndExit(parser):
    parser.print_help()


#
# modify a pair of lat or lon coordinates to correctly
# calcuate the shortest distance between them.
#
def wrapDistances(d1,d2):
  if d1 < -90 and d2 > 90:
    d2 = d2 - 360
  elif d2 < -90 and d1 > 90:
    d1 = d1 - 360
  return (d1,d2)


# compute the distance (in kilometers) between two points in lat / Ion
# this method makes use of the haversine formula of computing distance
def computeDistanceKM(lat1, lon1, lat2, lon2):
  #this computes distance in km
  (lat1,lat2) = wrapDistances(lat1,lat2)
  (lon1,lon2) = wrapDistances(lon1,lon2)
  R=6371
  dlat = math.radians(float(lat2)-float(lat1))
  dlon = math.radians(float(lon2)-float(lon1))
  a = float(math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2))
  c = float(2 * math.atan2(math.sqrt(a), math.sqrt(1-a)))
  return float(R * c)

def dateStrptime(dt):
  with contextlib.suppress(ValueError):
      return datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
  with contextlib.suppress(ValueError):
      return datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
  return None

configuration = AggregateMicroPathConfig(sys.argv.pop())
for line in sys.stdin:
  line = line.replace('\"','') # remove quotes
  #print line+"\n"
  
  #(user_id, dt, lat, lon) = line.strip().split("\t")
  (user_id, dt, lat, lon) = map(lambda x: x.strip(),line.split("\t"))
  try:
    dt = dt.split('.')[0]
    dt_parse = dateStrptime(dt)
    if not dt_parse:
        continue
  except ValueError:  # Guessing here, didn't like bare except.
      continue

  if current_user is None or current_user != user_id:
    current_user = user_id
    prevtime = dt_parse
    prevline = (user_id, dt_parse, lat, lon)
    hash_latlon = {}
    continue
  delta = dt_parse-prevline[1]
  total_time = float(delta.days*24*60*60+delta.seconds)
  #if too much time had passed... then skip the line
  if total_time > configuration.time_filter:  
    continue
  (auid,adt,alt,aln) = prevline
  (buid,bdt,blt,bln) = (user_id, dt_parse, lat, lon)
  
  try:
    alt = float(alt)
    aln = float(aln)
    blt = float(blt)
    bln = float(bln)
  except ValueError:  # again, avoiding bare except, just guessing
    continue

  distance = computeDistanceKM(alt, aln, blt, bln)

  #if the distance was too large, skip the segment
  if distance > configuration.distance_filter:
    continue

  #calculate km / hr
            
  latitude_diff = abs(float(alt) - float(blt))
  longitude_diff = abs(float(aln) - float(bln))

  #Make sure we actually went somewhere and didn't stay stationary
  if latitude_diff + longitude_diff > 0:
                
    hash_latlon[str(alt) + ',' + str(aln) + ',' + str(blt) + ',' + str(bln)] = 1
    segment = []
    segment.append(user_id)
    segment.append(str(alt))
    segment.append(str(blt))         
    segment.append(str(aln))
    segment.append(str(bln))
    segment.append(str(adt))
    segment.append(str(bdt))
    segment.append(str(total_time))
    segment.append(str(distance))
    
    if total_time == 0:
      segment.append('-1')
    else:
      segment.append(str(distance/(total_time/3600)))
    

    print("\t".join(segment))
     
  prevline = (user_id, dt_parse, lat, lon)             
         

    
    
