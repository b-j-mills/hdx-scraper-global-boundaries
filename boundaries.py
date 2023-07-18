import logging
import re
from geopandas import GeoDataFrame, read_file
from glob import glob
from os.path import join
from pandas.api.types import is_numeric_dtype
from shapely.geometry import MultiPolygon
from shapely.validation import make_valid
from topojson import Topology
from zipfile import BadZipFile, ZipFile

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.utilities.downloader import DownloadError
from hdx.utilities.uuid import get_uuid

logger = logging.getLogger()


def drop_fields(df, keep_fields):
    df = df.drop(
        [f for f in df.columns if f not in keep_fields and f.lower() != "geometry"],
        axis=1,
    )
    return df


class Boundaries:
    def __init__(
        self, configuration, downloader, temp_folder
    ):
        self.downloader = downloader
        self.boundaries = dict()
        self.UN_boundary = configuration["UN_boundaries"]["dataset"]
        self.boundary_names = configuration["UN_boundaries"]["resources"]
        self.temp_folder = temp_folder
        self.exceptions = {
            "dataset": configuration.get("dataset_exceptions", {}),
            "resource": configuration.get("resource_exceptions", {}),
        }
        self.headers = configuration["shapefile_attribute_mappings"]

    def download_boundary_inputs(self, levels):
        logger.info("Downloading boundaries")
        all_boundaries = dict()
        dataset = Dataset.read_from_hdx(self.UN_boundary)
        for resource in dataset.get_resources():
            if not resource["name"] in self.boundary_names.keys():
                continue
            if bool(re.match("polbnd[ap]_adm\d", resource["name"])) and not re.search("adm\d", resource["name"]).group() in levels:
                continue
            _, resource_file = resource.download(folder=self.temp_folder)
            lyr = read_file(resource_file)
            all_boundaries[self.boundary_names[resource["name"]]] = lyr

        self.boundaries = all_boundaries

    def create_national_boundary(self, iso):
        # select single country boundary (including disputed areas), cut out water, and dissolve
        country_adm0 = self.boundaries["adm0_polygon"].copy(deep=True)
        country_adm0 = country_adm0.loc[country_adm0["ISO_3"] == iso]
        country_adm0 = country_adm0.overlay(self.boundaries["water"], how="difference")
        country_adm0 = country_adm0.dissolve()
        country_adm0 = drop_fields(country_adm0, ["ISO_3"])
        country_adm0["ISO_3"] = iso
        if not country_adm0.crs:
            country_adm0 = country_adm0.set_crs(crs="EPSG:4326")
        if not country_adm0["geometry"][0].is_valid:
            country_adm0.loc[0, "geometry"] = make_valid(country_adm0.loc[0, "geometry"])
        return country_adm0

    def find_resource(self, iso, dataset, level):
        resource_name = self.exceptions["resource"].get(iso, "adm")
        boundary_resource = [
            r
            for r in dataset.get_resources()
            if r.get_file_type() == "shp"
            and bool(re.match(f".*{resource_name}.*", r["name"], re.IGNORECASE))
        ]

        if len(boundary_resource) > 1:
            name_match = [
                bool(re.match(f".*adm(in)?(\s)?(0)?{level[-1]}.*", r["name"], re.IGNORECASE))
                for r in boundary_resource
            ]
            boundary_resource = [
                boundary_resource[i]
                for i in range(len(boundary_resource))
                if name_match[i]
            ]
        return boundary_resource

    def find_shapefile(self, iso, resource, level):
        try:
            _, resource_file = resource.download(folder=self.temp_folder)
        except DownloadError:
            logger.error(f"{iso}: Could not download {level} resource")
            return None

        temp_folder = join(self.temp_folder, get_uuid())
        try:
            with ZipFile(resource_file, "r") as z:
                z.extractall(temp_folder)
        except BadZipFile:
            logger.error(f"{iso}: Could not unzip {level} file - it might not be a zip!")
            return None

        boundary_shp = glob(join(temp_folder, "**", "*.shp"), recursive=True)
        if len(boundary_shp) == 0:
            logger.error(f"{iso}: Did not find an {level} shapefile!")
            return None

        if len(boundary_shp) > 1:
            name_match = [
                bool(re.match(f".*admbnda.*adm(in)?(0)?{level[-1]}.*", b, re.IGNORECASE))
                for b in boundary_shp
            ]
            if any(name_match):
                boundary_shp = [boundary_shp[i] for i in range(len(boundary_shp)) if name_match[i]]

        if len(boundary_shp) > 1:
            simp_match = [bool(re.match(".*simplified.*", b, re.IGNORECASE)) for b in boundary_shp]
            if any(simp_match):
                boundary_shp = [boundary_shp[i] for i in range(len(boundary_shp)) if not simp_match[i]]

        if len(boundary_shp) != 1:
            logger.error(f"{iso}: Could not distinguish between {level} downloaded shapefiles")
            return None

        return boundary_shp[0]

    def calculate_fields(self, boundary_lyr, iso, country_name, req_fields, level):
        boundary_lyr["alpha_3"] = iso.upper()
        boundary_lyr["ADM0_REF"] = country_name

        fields = boundary_lyr.columns
        for l in range(1, int(level[-1]) + 1):
            possible_pcode_fields = [field.replace("#", str(l)) for field in self.headers["pcode"]]
            possible_name_fields = [field.replace("#", str(l)) for field in self.headers["name"]]
            pcode_field = None
            name_field = None
            if f"ADM{l}_PCODE" in fields:
                pcode_field = f"ADM{l}_PCODE"
            if f"ADM{l}_EN" in fields:
                name_field = f"ADM{l}_EN"
            for field in fields:
                if not pcode_field and field.upper() in possible_pcode_fields:
                    pcode_field = field
                if not name_field and field.upper() in possible_name_fields:
                    name_field = field

            if not name_field:
                boundary_lyr[f"ADM{l}_REF"] = ""
                logger.warning(f"{iso}: Could not map name field for adm{l}")

            else:
                boundary_lyr[f"ADM{l}_REF"] = boundary_lyr[name_field]

            # make sure pcodes are text
            if not pcode_field:
                boundary_lyr[f"ADM{l}_PCODE"] = ""
            if pcode_field:
                if is_numeric_dtype(boundary_lyr[pcode_field]):
                    boundary_lyr[f"ADM{l}_PCODE"] = (
                        boundary_lyr[pcode_field].astype(int).astype(str)
                    )
                else:
                    boundary_lyr[f"ADM{l}_PCODE"] = boundary_lyr[pcode_field]

        boundary_lyr = drop_fields(boundary_lyr, req_fields)
        boundary_lyr = boundary_lyr.dissolve(by=req_fields, as_index=False)
        return boundary_lyr

    def update_geometry(self, boundary_lyr, country_adm0, iso, level):
        # simplify geometry of boundaries
        boundary_topo = Topology(boundary_lyr)
        eps = 0.0075
        if int(level[-1]) > 0:
            eps = eps / int(level[-1])
        boundary_topo = boundary_topo.toposimplify(
            epsilon=eps,
            simplify_algorithm="dp",
            prevent_oversimplify=True,
        )
        boundary_lyr = boundary_topo.to_gdf(crs=country_adm0.crs)

        # make sure geometry is valid
        for i, _ in boundary_lyr.iterrows():
            if not boundary_lyr.geometry[i].is_valid:
                boundary_lyr.geometry[i] = make_valid(boundary_lyr.geometry[i])
            if boundary_lyr.geometry[i].geometryType() == "GeometryCollection":
                new_geom = []
                for part in boundary_lyr.geometry[i].geoms:
                    if part.geometryType() in ["Polygon", "MultiPolygon"]:
                        new_geom.append(part)
                if len(new_geom) == 0:
                    logger.error(f"{iso}: Boundary found with no geometry")
                if len(new_geom) == 1:
                    new_geom = new_geom[0]
                else:
                    new_geom = MultiPolygon(new_geom)
                boundary_lyr.geometry[i] = new_geom

        # clip international boundary to UN admin0 country boundary
        boundary_lyr = boundary_lyr.clip(mask=country_adm0, keep_geom_type=True)
        return boundary_lyr

    def replace_country_boundaries(self, boundaries, iso, level, geom_type):
        global_bounds = self.boundaries[f"{level}_{geom_type}"][
            self.boundaries[f"{level}_{geom_type}"]["alpha_3"] != iso
            ]
        global_bounds = global_bounds.append(boundaries)
        global_bounds.sort_values(by=[f"ADM{level[-1]}_PCODE"], inplace=True)
        self.boundaries[f"{level}_{geom_type}"] = global_bounds

    def update_subnational_boundaries(self, country, levels, do_not_process):
        iso = country["#country+code+v_iso3"]
        if iso in do_not_process:
            logger.warning(f"{iso}: Not processing for now")
            return None

        logger.info(f"{iso}: Processing {','.join(levels)} boundaries")

        # find the correct admin boundary dataset
        dataset_name = self.exceptions["dataset"].get(iso, f"cod-em-{iso.lower()}")
        dataset = Dataset.read_from_hdx(dataset_name)
        if not dataset:
            dataset = Dataset.read_from_hdx(f"cod-ab-{iso.lower()}")
        if not dataset:
            logger.error(f"{iso}: Could not find boundary dataset")
            return None

        country_adm0 = self.create_national_boundary(iso)

        for level in levels:
            req_fields = ["alpha_3", "ADM0_REF"]
            for i in range(1, int(level[-1]) + 1):
                req_fields.append(f"ADM{i}_PCODE")
                req_fields.append(f"ADM{i}_REF")

            # find correct admin boundary resource
            boundary_resource = self.find_resource(iso, dataset, level)
            if len(boundary_resource) == 0:
                logger.warning(f"{iso}: Could not find boundary resource at {level}")
                continue

            if len(boundary_resource) != 1:
                logger.error(f"{iso}: Could not distinguish between resources for {level}")
                continue

            # find the correct admin boundary shapefile in the downloaded zip
            boundary_shp = self.find_shapefile(iso, boundary_resource[0], level)
            if not boundary_shp:
                continue

            # read file and check projection
            boundary_lyr = read_file(boundary_shp)
            if not boundary_lyr.crs:
                boundary_lyr = boundary_lyr.set_crs(crs="EPSG:4326")
            if not boundary_lyr.crs.name == "WGS 84":
                boundary_lyr = boundary_lyr.to_crs(crs="EPSG:4326")

            # calculate fields, finding name and pcode fields from config
            boundary_lyr = self.calculate_fields(
                boundary_lyr,
                iso,
                country["#country+name+preferred"],
                req_fields,
                level,
            )
            na_count = boundary_lyr[f"ADM{level[-1]}_REF"].isna().sum()
            if na_count > 0:
                logger.warning(f"{iso}: Found {na_count} name null values at {level}")
            na_count = boundary_lyr[f"ADM{level[-1]}_PCODE"].isna().sum()
            if na_count > 0:
                logger.warning(f"{iso}: Found {na_count} pcode null values at {level}")

            # simplify geometry and harmonize with international boundary
            boundary_lyr = self.update_geometry(boundary_lyr, country_adm0, iso, level)

            # convert polygon boundaries to point
            points = GeoDataFrame(boundary_lyr.representative_point())
            points.rename(columns={0: "geometry"}, inplace=True)
            points[req_fields] = boundary_lyr[req_fields]
            points = points.set_geometry("geometry")

            # replace boundaries in global files
            self.replace_country_boundaries(boundary_lyr, iso, level, "polygon")
            self.replace_country_boundaries(points, iso, level, "point")

            logger.info(f"{iso}: Finished processing {level} boundaries")

    def update_subnational_resources(self, dataset_name, levels):
        dataset = Dataset.read_from_hdx(dataset_name)
        for level in levels:
            logger.info(f"Updating HDX datasets at {level}")
            polygon_name = [key for key in self.boundary_names if self.boundary_names[key] == f"{level}_polygon"]
            point_name = [key for key in self.boundary_names if self.boundary_names[key] == f"{level}_point"]
            polygon_file = join(self.temp_folder, polygon_name[0])
            self.boundaries[f"{level}_polygon"].to_file(polygon_file, driver="GeoJSON")
            point_file = join(self.temp_folder, point_name[0])
            self.boundaries[f"{level}_point"].to_file(point_file, driver="GeoJSON")

            resource_polygon = [r for r in dataset.get_resources() if r["name"] == polygon_name][0]
            resource_polygon.set_file_to_upload(polygon_file)
            resource_point = [r for r in dataset.get_resources() if r["name"] == point_name][0]
            resource_point.set_file_to_upload(point_file)

            try:
                resource_polygon.update_in_hdx()
            except HDXError:
                logger.exception("Could not update polygon resource")
            try:
                resource_point.update_in_hdx()
            except HDXError:
                logger.exception("Could not update point resource")
