# Copyright (c) 2019, Moritz E. Beber.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Provide namespace ETL functions."""


import json
import logging
from pathlib import Path
from typing import Dict, Set

from sqlalchemy.orm import sessionmaker

import httpx

from ..etl import extract_table
from ..model import IdentifiersOrgNamespaceModel, IdentifiersOrgRegistryModel


logger = logging.getLogger()
Session = sessionmaker()


def extract_prefixes(filename: Path) -> Set[str]:
    """

    Parameters
    ----------
    filename

    Returns
    -------

    """
    df = extract_table(filename)
    # TODO: Validate with goodtables.
    return set(df["prefix"].unique())


def et_namespaces() -> Dict[str, dict]:
    """
    Extract and transform all namespaces from the Identifiers.org registry.

    Returns
    -------
    dict
        A map from namespace prefixes to namespace objects.

    """
    logger.info("Downloading all namespaces...")
    response = httpx.get(
        "https://registry.api.identifiers.org/resolutionApi/getResolverDataset"
    )
    response.raise_for_status()
    logger.info("Extracting...")
    registry = IdentifiersOrgRegistryModel(**response.json())
    logger.info("Transforming...")
    return {
        namespace.prefix: namespace.dict() for namespace in registry.payload.namespaces
    }


def extract_namespace_mapping(
    filename: Path,
) -> Dict[str, IdentifiersOrgNamespaceModel]:
    """
    Extract a namespace mapping from a JSON file.

    Parameters
    ----------
    filename : pathlib.Path
        The path to the JSON file.

    Returns
    -------
    dict
        A map from namespace prefixes to Identifiers.org namespace data models.

    """
    with filename.open(mode="r") as handle:
        mapping = json.load(handle)
    old_value = IdentifiersOrgNamespaceModel.Config.allow_population_by_alias
    IdentifiersOrgNamespaceModel.Config.allow_population_by_alias = True
    mapping = {
        prefix: IdentifiersOrgNamespaceModel(**obj) for prefix, obj in mapping.items()
    }
    IdentifiersOrgNamespaceModel.Config.allow_population_by_alias = old_value
    return mapping
