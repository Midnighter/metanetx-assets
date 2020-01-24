# Copyright (c) 2019-2020, Moritz E. Beber.
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

import logging
from typing import Dict, List, Set

import httpx
from cobra_component_models.orm import Namespace
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from ..etl import patch_namespace
from ..model import IdentifiersOrgNamespaceModel, IdentifiersOrgRegistryModel


logger = logging.getLogger()
Session = sessionmaker()


def download_namespace_mapping(
    url: str = "https://registry.api.identifiers.org/resolutionApi/getResolverDataset",
) -> Dict[str, dict]:
    """
    Extract all namespaces from the Identifiers.org registry.

    Returns
    -------
    dict
        A map from namespace prefixes to namespace objects.

    """
    response = httpx.get(url)
    response.raise_for_status()
    registry = IdentifiersOrgRegistryModel.parse_raw(response.text)
    return {
        namespace.prefix: namespace.dict() for namespace in registry.payload.namespaces
    }


def transform_namespaces(
    namespace_mapping: Dict[str, IdentifiersOrgNamespaceModel], prefixes: Set[str]
):
    models: List[Namespace] = []
    for prefix in tqdm(prefixes, desc="Namespace"):
        try:
            models.append(Namespace(**namespace_mapping[prefix].dict()))
        except KeyError:
            model = patch_namespace(prefix)
            if model:
                models.append(model)
    return models
