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
from pathlib import Path
import logging

class AggregateMicroPathConfig:
    """Reads AMP config file and creates object with AMP config vars.
    TODO: Replace with toml.  Reduce boilerplate?

    """
    def __init__(self, config, basePath="./"):
        self.config_file = Path(basePath).resolve() / config
        self.parser = self.get_config_parser()
        self.parse_config()

    def get_config_parser(self) -> SafeConfigParser:
        parser = SafeConfigParser()
        parser.read(self.config_file)
        if not parser.sections():
            logging.error(f"""
                Config file is empty or has no sections. Wrong folder?
                -> Config File: {self.config_file}.
                -> Sections: {parser.sections()}"""
                )
        return parser
        
    def parse_config(self) -> None:
        AMP = "AggregateMicroPath"
        parser = self.parser

        self.database_name = parser.get(AMP, "database_name")
        self.table_name = parser.get(AMP, "table_name")
        self.table_schema_id = parser.get(AMP, "table_schema_id")
        self.table_schema_dt = parser.get(AMP, "table_schema_dt")
        self.table_schema_lat = parser.get(AMP, "table_schema_lat")
        self.table_schema_lon = parser.get(AMP, "table_schema_lon")
        self.time_filter = parser.get(AMP, "time_filter")
        self.distance_filter = parser.get(AMP, "distance_filter")
        self.tripLat1 = float(parser.get(AMP, "lower_left_lat"))
        self.tripLon1 = float(parser.get(AMP, "lower_left_lon"))
        self.tripLat2 = float(parser.get(AMP, "upper_right_lat"))
        self.tripLon2 = float(parser.get(AMP, "upper_right_lon"))
        self.tripname = parser.get(AMP, "trip_name")
        self.resolutionLat = float(parser.get(AMP, "resolution_lat"))
        self.resolutionLon = float(parser.get(AMP, "resolution_lon"))
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
        self.temporal_split = parser.get(AMP, "temporal_split")
