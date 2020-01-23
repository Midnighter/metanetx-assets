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


"""Provide compartment ETL functions."""


import logging
from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from cobra_component_models.orm import Compartment, Namespace

from .. import etl
from ..model import IdentifiersOrgNamespaceModel


logger = logging.getLogger()
Session = sessionmaker()


def extract_compartment_properties(filename: Path) -> pd.DataFrame:
    """
    Extract a MetaNetX table with compartment property information.

    Parameters
    ----------
    filename : pathlib.Path
        The path to the tabular file.

    Returns
    -------
    pandas.DataFrame
        A data frame with compartment properties.

    """
    comp_prop = pd.read_csv(str(filename), sep="\t")
    # TODO (Moritz): Validate the tabular data using goodtables or similar.
    return comp_prop


def extract_compartment_cross_references(filename: Path) -> pd.DataFrame:
    """
    Extract a MetaNetX table with compartment cross-references.

    Parameters
    ----------
    filename : pathlib.Path
        The path to the tabular file.

    Returns
    -------
    pandas.DataFrame
        A data frame with compartment cross-references.

    """
    comp_xref = pd.read_csv(str(filename), sep="\t")
    # TODO (Moritz): Validate the tabular data using goodtables or similar.
    return comp_xref


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
def load_compartments(
    session,
    namespaces: Dict[str, IdentifiersOrgNamespaceModel],
    comp_prop: pd.DataFrame,
    comp_xref: pd.DataFrame,
    batch_size: int = 1000,
):
    """
    Load compartment information into a database.

    Parameters
    ----------
    session
    namespaces
    comp_prop
    comp_xref
    batch_size : int

    """
    prefixes = set(comp_prop["prefix"].unique()).union(comp_xref["prefix"].unique())
    mapping = {p: namespaces[p] for p in prefixes}
    etl.load_missing_namespaces(session, mapping)
    mapping = Namespace.get_map(session, prefixes)
    # load_compartments()
    grouped_xref = comp_xref.groupby("mnx_id", sort=False)
    with tqdm(total=len(comp_prop), desc="Compartments") as pbar:
        for index in range(0, len(comp_prop), batch_size):
            compartments = []
            for row in comp_prop[index : index + batch_size].itertuples(index=False):
                names = {}
                annotation = {}
                comp = Compartment()
                names.setdefault(row.prefix, set(row.description.split("|")))
                annotation.setdefault("metanetx.compartment", {row.mnx_id})
                annotation.setdefault(row.prefix, set())
