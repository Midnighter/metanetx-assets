# The MIT License (MIT)
#
# Copyright (c) 2018 Institute for Molecular Systems Biology, ETH Zurich.
# Copyright (c) 2018 Novo Nordisk Foundation Center for Biosustainability,
# Technical University of Denmark
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


"""Populate and fix information on Identifiers.org namespaces."""


import logging
from pathlib import Path
from typing import Dict, List, Tuple, Set

import pandas as pd
import requests
from tqdm import tqdm

import httpx
from cobra_component_models.orm import Namespace

from ..model import IdentifiersOrgNamespaceModel


logger = logging.getLogger(__name__)


def load_missing_namespaces(
    session, namespaces: Dict[str, IdentifiersOrgNamespaceModel]
):
    """

    Parameters
    ----------
    session :
    namespaces : dict

    Returns
    -------
    dict

    """
    prefixes = set(namespaces)
    existing = {
        row.prefix
        for row in session.query(Namespace.prefix).filter(
            Namespace.prefix.in_(prefixes)
        )
    }
    for prefix in prefixes.difference(existing):
        session.add(Namespace(**namespaces[prefix].dict()))
    session.commit()


async def request_namespaces(
    prefixes: List[str],
) -> List[Tuple[str, IdentifiersOrgNamespaceModel]]:
    namespaces = []
    with httpx.AsyncClient(
        base_url="https://registry.api.identifiers.org/restApi/namespaces/search/findByPrefix"
    ) as client:
        for prefix in tqdm(prefixes, desc="Requests"):
            request = client.build_request("GET", url="", params={"prefix": prefix})
            response = await client.send(request)
            response.raise_for_status()
            namespace = IdentifiersOrgNamespaceModel(**response.json())
            namespaces.append(Namespace(**namespace.dict()))
    return namespaces


def load_namespaces(
    namespaces: List[Tuple[str, IdentifiersOrgNamespaceModel]], session
) -> Dict[str, Namespace]:
    namespace_map = {}
    for prefix, ns_model in tqdm(namespaces, desc="Namespace"):
        namespace_map[prefix] = namespace = Namespace(**ns_model.dict())
        session.add(namespace)
    session.commit()
    return namespace_map


def load_rest_data(url: str) -> dict:
    """Return the object from a simple GET request."""
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


# def create_registry(entry: dict) -> Registry:
#     """Create a new registry."""
#     registry = Registry(
#         namespace=entry["prefix"],
#         name=entry.get("name"),
#         pattern=entry.get("pattern"),
#         identifier=entry.get("id"),
#         url=entry.get("url"),
#         is_prefixed=bool(entry.get("prefixed", False)),
#     )
#     try:
#         registry.access_url = entry.get("resources", [])[0]["accessURL"]
#     except IndexError:
#         pass
#     return registry


def patch_registry(session, prefix: str) -> None:
    """Create an entry similar to a registry defined by identifiers.org."""
    entry = {"namespace": prefix, "is_prefixed": False}
    if prefix == "envipath":
        entry["name"] = "enviPath"
        entry["pattern"] = r"^.+$"
        entry["access_url"] = "https://envipath.org/package/{$id}"
    elif prefix == "synonyms":
        entry["name"] = "Synonyms"
        entry["pattern"] = r"^.+$"
    elif prefix == "coco":
        entry["name"] = "Component-Contribution Metabolite"
        entry["pattern"] = r"^COCOM\d+$"
    else:
        raise ValueError(f"Unknown registry prefix '{prefix}'.")
    # We use low-level insertion in order to circumvent the validation.
    session.execute(Registry.__table__.insert(), [entry])


def populate_registries(session, cross_references: pd.DataFrame) -> None:
    """Populate the database with registry information."""
    prefixes = set(cross_references["prefix"].unique())
    # Add dummy registry for names.
    prefixes.add("synonyms")
    # Add dummy registry for additional compounds.
    prefixes.add("coco")
    # remove deprecated
    prefixes.remove("deprecated")

    # Load registry information from identifiers.org.
    collections = {
        c["prefix"]: c["id"]
        for c in load_rest_data("http://identifiers.org/rest/collections")
    }
    with open_text(equilibrator_assets.data, "prefix_mapping.tsv") as handle:
        mapping = {
            row.mnx_prefix: row.identifiers_prefix
            for row in pd.read_table(handle, header=0).itertuples(index=False)
        }
    for prefix in tqdm(prefixes, total=len(prefixes), desc="Registry"):
        id_prefix = mapping.get(prefix)

        if id_prefix is None:
            logger.warning(f"Prefix '{prefix}' not present in mapping.")
            patch_registry(session, prefix)
            continue
        elif id_prefix not in collections:
            logger.error(f"Prefix '{prefix}' does not exist at identifiers.org.")
            continue
        else:
            miriam = collections[id_prefix]
            entry = load_rest_data(f"http://identifiers.org/rest/collections/{miriam}")
        session.add(create_registry(entry))
    session.commit()


def get_mnx_mapping(session):
    """Return a mapping from MetaNetX prefixes to MIRIAM registries."""
    with open_text(equilibrator_assets.data, "prefix_mapping.tsv") as handle:
        mapping = {
            row.mnx_prefix: session.query(Registry)
            .filter_by(namespace=row.identifiers_prefix)
            .one_or_none()
            for row in pd.read_csv(handle, sep="\t", header=0).itertuples(index=False)
        }
    mapping["envipath"] = (
        session.query(Registry).filter_by(namespace="envipath").one_or_none()
    )
    mapping["synonyms"] = (
        session.query(Registry).filter_by(namespace="synonyms").one_or_none()
    )
    mapping["deprecated"] = (
        session.query(Registry).filter_by(namespace="metanetx.chemical").one_or_none()
    )
    return mapping
