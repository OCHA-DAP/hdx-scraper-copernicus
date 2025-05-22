#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
import sys
from os import getenv
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

from hdx.scraper.copernicus.ghsl import GHSL
from hdx.scraper.copernicus.utilities import get_boundaries

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-copernicus"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: copernicus"

generate_country_datasets = True
generate_global_datasets = True


def main(
    save: bool = True,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to True.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """
    configuration = Configuration.read()
    if not User.check_current_user_organization_access("copernicus", "create_dataset"):
        raise PermissionError(
            "API Token does not give access to Copernicus organisation!"
        )

    running_on_gha = False if getenv("GITHUB_ACTIONS") is None else True
    with wheretostart_tempdir_batch(folder=_USER_AGENT_LOOKUP) as info:
        temp_dir = info["folder"]
        today = now_utc()
        year = today.year
        with Download() as downloader:
            retriever = Retrieve(
                downloader=downloader,
                fallback_dir=temp_dir,
                saved_dir=_SAVED_DATA_DIR,
                temp_dir=temp_dir,
                save=save,
                use_saved=use_saved,
            )
            ghsl = GHSL(configuration["ghsl"], retriever)
            ghsl_updated = ghsl.get_ghs_data(
                year,
                generate_country_datasets,
                running_on_gha,
            )
            global_boundaries = get_boundaries(configuration, retriever, temp_dir)

            if ghsl_updated and not running_on_gha:
                if generate_global_datasets:
                    dataset = ghsl.generate_global_dataset()
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    dataset["notes"] = dataset["notes"].replace("\n", "  \n")
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        match_resource_order=False,
                        hxl_update=False,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=info["batch"],
                    )

                if generate_country_datasets:
                    ghsl.get_tiling_schema()
                    iso3s = ghsl.get_boundaries(global_boundaries)

                    for iso3 in iso3s:
                        if iso3 in ["ATA"]:
                            continue
                        country_data = ghsl.process(iso3)
                        if not country_data:
                            continue
                        dataset = ghsl.generate_dataset(iso3)
                        dataset.update_from_yaml(
                            script_dir_plus_file(
                                join("config", "hdx_dataset_static.yaml"), main
                            )
                        )
                        dataset["notes"] = dataset["notes"].replace("\n", "  \n")
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            match_resource_order=False,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                            batch=info["batch"],
                        )

            if ghsl_updated and running_on_gha:
                logger.error("GHSL data has been updated, run locally")
                sys.exit(1)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
