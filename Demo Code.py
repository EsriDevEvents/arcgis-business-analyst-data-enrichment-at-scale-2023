"""ArcGIS Business Analyst Data Enrichment at scale.

In this code snippet we will use the Business Analyst extension for ArcGIS 
Pro to generate a tessellation for a nationwide dataset and enrich it with 
hundreds of variables. We will use Geoprocessing tools from ArcGIS Pro and 
the ArcGIS API for Python with ArcGIS Notebooks to build an effective 
pipeline for preparing data, selecting variables, and performing enrichment 
in an effective and scalable way.

Copyright 2023 Esri
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

import arcpy, os, time
from arcgis.geoenrichment import Country
from arcgis import GIS
import pandas as pd

gis = GIS('pro')

usa = Country('USA', gis = gis)

us_enrich_variables = usa.enrich_variables

us_filtered_variables = us_enrich_variables[us_enrich_variables['name'].str.match(r"THH[0-9][0-9]")]
us_filtered_variables

start_time = time.time()

with arcpy.EnvManager(baDataSource="LOCAL;;USA_ESRI_2022"):
    states_fc = "memory/States"
    dissolved_states_fc = "memory/DissolvedStates"
    hexbins_fc = r"C:\Developer Summit Demo\Dev Summit 2023 Demo\Dev Summit 2023 Demo.gdb\script_result_hexbins"
    enriched_hexbins_fc = r"C:\Developer Summit Demo\Dev Summit 2023 Demo\Dev Summit 2023 Demo.gdb\script_result_hexbins_enriched"

    if arcpy.Exists(states_fc):
        arcpy.management.Delete(states_fc)

    if arcpy.Exists(dissolved_states_fc):
        arcpy.management.Delete(dissolved_states_fc)

    if arcpy.Exists(hexbins_fc):
        arcpy.management.Delete(hexbins_fc)

    if arcpy.Exists(enriched_hexbins_fc):
        arcpy.management.Delete(enriched_hexbins_fc)

    arcpy.ba.StandardGeographyTA(
        geography_level="US.States",
        out_feature_class=states_fc,
        input_type="LIST",
        in_ids_table=None,
        geography_key_field=None,
        ids_list="01,04,05,06,08,09,10,11,12,13,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,53,54,55,56",
        summarize_duplicates="USE_FIRST",
        group_field=None,
        dissolve_output="DONT_DISSOLVE"
    )

    print(f"{time.time() - start_time} Finished Standard Geography")

    arcpy.management.Dissolve(
        in_features=states_fc,
        out_feature_class=dissolved_states_fc,
        dissolve_field=None,
        statistics_fields=None,
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES",
        concatenation_separator=""
    )

    print(f"{time.time() - start_time} Finished Dissolve")

    arcpy.edit.Generalize(
        in_features=dissolved_states_fc,
        tolerance="2 Kilometers"
    )

    print(f"{time.time() - start_time} Finished Generalize")

    arcpy.ba.GenerateGridsAndHexagons(
        area_of_interest=dissolved_states_fc,
        out_feature_class=hexbins_fc,
        cell_type="H3_HEXAGON",
        enrich_type="",
        cell_size="1 SquareMiles",
        h3_resolution=7,
        variables=None,
        distance_type="STRAIGHT_LINE_DISTANCE",
        distance=1,
        units="KILOMETERS",
        out_enriched_buffers=None,
        travel_direction="TOWARD_STORES",
        time_of_day=None,
        time_zone="TIME_ZONE_AT_LOCATION",
        search_tolerance=None,
        polygon_detail="STANDARD"
    )

    print(f"{time.time() - start_time} Finished Generate Grids and Hexagons")
    print(arcpy.GetMessages())

    variables_string = ";".join(us_filtered_variables["enrich_name"].tolist())

    arcpy.analysis.Enrich(
        in_features=hexbins_fc,
        out_feature_class=enriched_hexbins_fc,
        variables=variables_string,
        buffer_type="",
        distance=1,
        unit=""
    )

    print(f"{time.time() - start_time} Finished Standard Geography")
    print(arcpy.GetMessages())

    fields = ["GRID_ID"] + us_filtered_variables["enrich_field_name"].tolist()
    cursor = arcpy.da.SearchCursor(enriched_hexbins_fc, fields)
    data = [row for row in cursor]
    df = pd.DataFrame(data, columns=fields)
    
    parquet_file = r"C:\Developer Summit Demo\result.parquet"
    if os.path.exists(parquet_file):
        os.remove(parquet_file)
    df.to_parquet(parquet_file)

    print(f"{time.time() - start_time} Finished Parquet Export")