#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this
script then creates in HDX.

"""

import logging
from copy import deepcopy
from os.path import dirname, expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve
from hdx.utilities.state import State

from hdx.scraper.copernicus.copernicus import Copernicus

logger = logging.getLogger(__name__)

_USER_AGENT_LOOKUP = "hdx-scraper-copernicus"
_SAVED_DATA_DIR = "saved_data"  # Keep in repo to avoid deletion in /tmp
_UPDATED_BY_SCRIPT = "HDX Scraper: copernicus"

generate_country_datasets = True
generate_global_dataset = True


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
        raise PermissionError("API Token does not give access to Copernicus organisation!")

    with State(
        "data_dates.txt",
        State.dates_str_to_country_date_dict,
        State.country_date_dict_to_dates_str,
    ) as state:
        state_dict = deepcopy(state.get())
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
                copernicus = Copernicus(
                    configuration,
                    retriever,
                )
                updated = copernicus.get_ghs_data(
                    year,
                    state_dict,
                    generate_country_datasets,
                )

                if updated and generate_global_dataset:
                    dataset = copernicus.generate_global_dataset()
                    dataset.update_from_yaml(
                        path=join(dirname(__file__), "config", "hdx_dataset_static.yaml")
                    )
                    dataset["notes"] = dataset["notes"].replace("\n", "  \n")
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        match_resource_order=False,
                        hxl_update=False,
                        updated_by_script=_UPDATED_BY_SCRIPT,
                        batch=info["batch"],
                    )

                if updated and generate_country_datasets:
                    copernicus.get_tiling_schema()
                    iso3s = copernicus.get_boundaries()

                    for iso3 in iso3s:
                        if iso3 in ["ATA"]:
                            continue
                        country_data = copernicus.process(iso3)
                        if not country_data:
                            continue
                        dataset = copernicus.generate_dataset(iso3)
                        dataset.update_from_yaml(
                            path=join(dirname(__file__), "config", "hdx_dataset_static.yaml")
                        )
                        dataset["notes"] = dataset["notes"].replace("\n", "  \n")
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            match_resource_order=False,
                            hxl_update=False,
                            updated_by_script=_UPDATED_BY_SCRIPT,
                            batch=info["batch"],
                        )
                state.set(state_dict)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_USER_AGENT_LOOKUP,
        project_config_yaml=join(dirname(__file__), "config", "project_configuration.yaml"),
    )
