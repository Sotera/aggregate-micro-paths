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

import math
from ConfigParser import SafeConfigParser

class AggregateMicroPathConfig:
    
    config_file = ""
    table_name = ""
    table_schema_id = ""
    table_schema_dt = ""
    table_schema_lat = ""
    table_schema_lon = ""
    time_filter = 0
    distance_filter = 0
    tripLat1 = 0
    tripLon1 = 0
    tripLat2 = 0
    tripLon2 = 0
    tripname = ""
    resolutionLat = 0
    resolutionLon = 0
    tripLatMin = 0
    tripLatMax = 0
    tripLonMin = 0
    tripLonMax = 0
    triplineBlankets = []
    
    def __init__(self, config, basePath = "./"):
        configParser = SafeConfigParser()
        configParser.read(basePath + config)
        self.config_file = config 
        self.database_name = configParser.get("AggregateMicroPath", "database_name")
        self.table_name = configParser.get("AggregateMicroPath", "table_name") 
        self.table_schema_id = configParser.get("AggregateMicroPath", "table_schema_id") 
        self.table_schema_dt = configParser.get("AggregateMicroPath", "table_schema_dt") 
        self.table_schema_lat = configParser.get("AggregateMicroPath", "table_schema_lat") 
        self.table_schema_lon = configParser.get("AggregateMicroPath", "table_schema_lon") 
        self.time_filter = long(configParser.get("AggregateMicroPath", "time_filter")) 
        self.distance_filter = long(configParser.get("AggregateMicroPath", "distance_filter"))
        self.tripLat1 = float(configParser.get("AggregateMicroPath", "lower_left_lat")) 
        self.tripLon1 = float(configParser.get("AggregateMicroPath", "lower_left_lon"))
        self.tripLat2 = float(configParser.get("AggregateMicroPath", "upper_right_lat")) 
        self.tripLon2 = float(configParser.get("AggregateMicroPath", "upper_right_lon"))
        self.tripname = configParser.get("AggregateMicroPath", "trip_name") 
        self.resolutionLat = float(configParser.get("AggregateMicroPath", "resolution_lat"))
        self.resolutionLon = float(configParser.get("AggregateMicroPath", "resolution_lon"))
        self.tripLatMin = int(math.floor(self.tripLat1/self.resolutionLat))#6
        self.tripLatMax = int(math.ceil(self.tripLat2/self.resolutionLat)) #7
        self.tripLonMin = int(math.floor(self.tripLon1/self.resolutionLon)) #8
        self.tripLonMax = int(math.ceil(self.tripLon2/self.resolutionLon)) #9
        self.triplineBlankets.append([self.tripLat1,self.tripLon1,self.tripLat2,self.tripLon2,self.tripname,self.resolutionLat,self.resolutionLon,self.tripLatMin,self.tripLatMax,self.tripLonMin,self.tripLonMax])
        self.temporal_split = configParser.get("AggregateMicroPath", "temporal_split") 
        
        

