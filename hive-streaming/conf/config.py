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
from configparser import SafeConfigParser


class AggregateMicroPathConfig:
    # Removed bunch of class vars initialized to 0 or empty.
    # They're clobbered by __init__ anyway, and probably shouldn't
    # be class vars.

    def __init__(self, config, basePath="./"):
        AMP = "AggregateMicroPath"
        configParser = SafeConfigParser()
        configParser.read(basePath + config)
        self.config_file = config
        self.database_name = configParser.get(AMP, "database_name")
        self.table_name = configParser.get(AMP, "table_name")
        self.table_schema_id = configParser.get(AMP, "table_schema_id")
        self.table_schema_dt = configParser.get(AMP, "table_schema_dt")
        self.table_schema_lat = configParser.get(AMP, "table_schema_lat")
        self.table_schema_lon = configParser.get(AMP, "table_schema_lon")
        self.time_filter = configParser.get(AMP, "time_filter")
        self.distance_filter = configParser.get(AMP, "distance_filter")
        self.tripLat1 = float(configParser.get(AMP, "lower_left_lat"))
        self.tripLon1 = float(configParser.get(AMP, "lower_left_lon"))
        self.tripLat2 = float(configParser.get(AMP, "upper_right_lat"))
        self.tripLon2 = float(configParser.get(AMP, "upper_right_lon"))
        self.tripname = configParser.get(AMP, "trip_name")
        self.resolutionLat = float(configParser.get(AMP, "resolution_lat"))
        self.resolutionLon = float(configParser.get(AMP, "resolution_lon"))
        self.tripLatMin = int(math.floor(self.tripLat1 / self.resolutionLat))  # 6
        self.tripLatMax = int(math.ceil(self.tripLat2 / self.resolutionLat))  # 7
        self.tripLonMin = int(math.floor(self.tripLon1 / self.resolutionLon))  # 8
        self.tripLonMax = int(math.ceil(self.tripLon2 / self.resolutionLon))  # 9
        self.triplineBlankets = [
            self.tripLat1,
            self.tripLon1,
            self.tripLat2,
            self.tripLon2,
            self.tripname,
            self.resolutionLat,
            self.resolutionLon,
            self.tripLatMin,
            self.tripLatMax,
            self.tripLonMin,
            self.tripLonMax,
        ]
        self.temporal_split = configParser.get(AMP, "temporal_split")
