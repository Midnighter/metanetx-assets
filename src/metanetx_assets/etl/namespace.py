# The MIT License (MIT)
#
# Copyright (c) 2019-2020, Moritz E. Beber.
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
from typing import Dict, Literal, Optional, Set

import pandas as pd
from cobra_component_models.orm import Namespace
from pydantic import parse_file_as

from ..model import IdentifiersOrgNamespaceModel


logger = logging.getLogger(__name__)


def patch_namespace(prefix: Literal["envipath", "name"]) -> Optional[Namespace]:
    """Create an entry similar to a registry defined by identifiers.org."""
    if prefix == "name":
        logger.warning(
            "The prefix 'name' is not a namespace in the Identifiers.org registry. "
            "Ignored."
        )
        return None
    kwargs = {"prefix": prefix, "embedded_prefix": False}
    if prefix == "envipath":
        logger.warning(
            "The prefix 'envipath' is not yet contained in the Identifiers.org "
            "registry. Creating a placeholder."
        )
        model = Namespace(
            miriam_id="MIR:00000000",
            name="enviPath",
            pattern=r"^.+$",
            description="A placeholder until envipath is added to the Identifiers.org "
            "registry.",
            **kwargs,
        )
    else:
        raise ValueError(f"Unknown namespace prefix '{prefix}'.")
    return model


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
    old_value = IdentifiersOrgNamespaceModel.Config.allow_population_by_field_name
    IdentifiersOrgNamespaceModel.Config.allow_population_by_field_name = True
    mapping = parse_file_as(Dict[str, IdentifiersOrgNamespaceModel], filename)
    IdentifiersOrgNamespaceModel.Config.allow_population_by_field_name = old_value
    return mapping


def get_unique_prefixes(data_frame: pd.DataFrame) -> Set[str]:
    """

    Parameters
    ----------
    data_frame : pandas.DataFrame

    Returns
    -------
    set

    """
    return set(data_frame["prefix"].unique())


def get_required_prefixes(
    chem_prop: pd.DataFrame,
    chem_xref: pd.DataFrame,
    comp_prop: pd.DataFrame,
    comp_xref: pd.DataFrame,
    reac_prop: pd.DataFrame,
    reac_xref: pd.DataFrame,
) -> Set[str]:
    """

    Parameters
    ----------
    chem_prop
    chem_xref
    comp_prop
    comp_xref
    reac_prop
    reac_xref

    Returns
    -------
    set
        The set of prefixes for Identifiers.org namespaces used in all of the MetaNetX
        tables.

    """
    prefixes = set()
    prefixes.update(get_unique_prefixes(chem_prop))
    prefixes.update(get_unique_prefixes(chem_xref))
    prefixes.update(get_unique_prefixes(comp_prop))
    prefixes.update(get_unique_prefixes(comp_xref))
    prefixes.update(get_unique_prefixes(reac_prop))
    prefixes.update(get_unique_prefixes(reac_xref))
    return prefixes
