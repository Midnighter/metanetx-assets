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


"""Populate compound information."""


import logging
import typing

import pandas as pd


logger = logging.getLogger(__name__)


def create_compound_identifier_objects(
    compound_id: int,
    cross_references: pd.DataFrame,
    prefix2registry: typing.Dict[str, Registry],
) -> typing.List[dict]:
    """Generate compound cross-references for bulk insertion."""
    identifiers = []
    for row in cross_references.itertuples(index=False):
        registry = prefix2registry[row.prefix]
        if registry is None:
            continue
        if registry.is_prefixed:
            accession = f"{row.prefix.upper()}:{row.accession}"
        else:
            accession = row.accession
        identifiers.append(
            {
                "compound_id": compound_id,
                "registry_id": registry.id,
                "accession": accession,
            }
        )
    registry = prefix2registry["synonyms"]
    names = cross_references.loc[
        cross_references["description"].notnull(), "description"
    ].unique()
    for name in names:
        identifiers.append(
            {"compound_id": compound_id, "registry_id": registry.id, "accession": name}
        )

    return identifiers


def load_compartments(
    session, properties: pd.DataFrame, cross_references: pd.DataFrame, batch_size: int
) -> None:
    """
    Populate the compound and identifier tables using information from MetaNetX.

    Parameters
    ----------
    session : SQLAlchemy.Session
    properties : pd.DataFrame
    cross_references : pd.DataFrame
    batch_size : int

    Warnings
    --------
    The function uses bulk inserts for performance and thus assumes empty
    tables. Do **not** use it for updating content.

    """
    prefix2registry = get_mnx_mapping(session)
    grouped_xref = cross_references[
        (cross_references["prefix"] != "metanetx.chemical")
        & cross_references["accession"].notnull()
    ].groupby("mnx_id", sort=False)
    with tqdm(total=len(properties), desc="Compounds") as pbar:
        for index in range(0, len(properties), batch_size):
            compounds = [
                create_compound_object(row)
                for row in properties[index : index + batch_size].itertuples(
                    index=False
                )
            ]
            session.bulk_insert_mappings(Compound, compounds)
            session.commit()
            pbar.update(len(compounds))
    with tqdm(total=len(properties), desc="Cross-References") as pbar:
        for index in range(0, len(properties), batch_size):
            identifiers = []
            counter = 0
            for row in session.query(Compound.id, Compound.mnx_id).slice(
                index, index + batch_size
            ):
                try:
                    identifiers.extend(
                        create_compound_identifier_objects(
                            row.id, grouped_xref.get_group(row.mnx_id), prefix2registry
                        )
                    )
                except KeyError:
                    logger.debug("Compound '%s' has no cross-references.", row.mnx_id)
                counter += 1
            session.bulk_insert_mappings(CompoundIdentifier, identifiers)
            session.commit()
            pbar.update(counter)
