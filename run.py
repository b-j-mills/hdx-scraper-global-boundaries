import argparse
import logging
import warnings
from os.path import expanduser, join
from shapely.errors import ShapelyDeprecationWarning

from hdx.api.configuration import Configuration
from hdx.location.country import Country
from hdx.facades.keyword_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import temp_dir
from boundaries import Boundaries

setup_logging()
logger = logging.getLogger(__name__)
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)

lookup = "hdx-scraper-viz-inputs"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-co", "--countries_override", default=None, help="Which countries to update")
    parser.add_argument("-lv", "--levels", default=None, help="Which levels to update")
    args = parser.parse_args()
    return args


def main(
    countries_override,
    levels,
    **ignore,
):
    logger.info(f"##### hdx-viz-data-inputs ####")
    configuration = Configuration.read()
    with temp_dir(folder="TempDataExplorerInputs") as temp_folder:
        with Download(rate_limit={"calls": 1, "period": 0.1}) as downloader:

            countries = Country.countriesdata()["countries"]
            if countries_override:
                countries = {
                    country.upper(): countries.get(country.upper()) for country in countries_override
                }

            if not levels:
                levels = configuration["levels"]

            boundaries = Boundaries(
                configuration,
                downloader,
                temp_folder,
            )

            boundaries.download_boundary_inputs(levels)

            for country in countries:
                boundaries.update_subnational_boundaries(
                    countries[country],
                    levels,
                    configuration.get("do_not_process", []),
                )
            boundaries.update_subnational_resources(configuration["UN_boundaries"]["dataset"], levels)
            logger.info("Finished processing!")


if __name__ == "__main__":
    args = parse_args()
    countries_override = args.countries_override
    if countries_override:
        countries_override = countries_override.split(",")
    levels = args.levels
    if levels:
        levels = levels.split(",")
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
        countries_override=countries_override,
        levels=levels,
    )
