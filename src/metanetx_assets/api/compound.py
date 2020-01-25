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


"""Provide high-level compound ETL functions."""


import logging
from typing import Dict

import pandas as pd
from cobra_component_models.orm import (
    BiologyQualifier,
    Compound,
    CompoundAnnotation,
    CompoundName,
    Namespace,
)
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm


logger = logging.getLogger()
Session = sessionmaker()


def etl_compounds(
    session: Session,
    compounds: pd.DataFrame,
    cross_references: pd.DataFrame,
    mapping: Dict[str, Namespace],
    qualifier: BiologyQualifier,
    batch_size: int = 1000,
):
    """
    Load compartment information into a database.

    Parameters
    ----------
    session
    compounds
    cross_references
    mapping
    qualifier
    batch_size : int

    """
    # TODO: This is a first draft of the function. Parts should be refactored to the
    #  etl sub-package so that they can be tested better.
    grouped_xref = cross_references.groupby("mnx_id", sort=False)
    # The InChI field (and thus also InChIKey) must be unique since it is the same
    # structure.
    is_duplicated = compounds.duplicated("inchi_key") & compounds["inchi_key"].notnull()
    deduped = compounds.loc[~is_duplicated, :]
    with tqdm(total=len(deduped), desc="Compound") as pbar:
        for index in range(0, len(deduped), batch_size):
            models = []
            for row in deduped.iloc[index : index + batch_size, :].itertuples(
                index=False
            ):
                logger.debug(row.mnx_id)
                comp = Compound(
                    inchi=row.inchi,
                    inchi_key=row.inchi_key,
                    smiles=row.smiles,
                    chemical_formula=row.formula,
                    charge=row.charge,
                    mass=row.mass,
                )
                # We collect names and identifiers such that we insert only
                # unique names per namespace.
                names = {}
                identifiers = {}
                # We avoid NaN (missing) values here.
                if isinstance(row.description, str):
                    names[row.prefix] = {n.strip() for n in row.description.split("|")}
                identifiers["metanetx.chemical"] = {row.mnx_id}
                identifiers.setdefault(row.prefix, set()).add(row.identifier)
                # Expand names and identifiers with cross-references.
                for xref_row in grouped_xref.get_group(row.mnx_id).itertuples(
                    index=False
                ):
                    # We avoid NaN (missing) values here.
                    if isinstance(xref_row.description, str):
                        names.setdefault(xref_row.prefix, set()).update(
                            (n.strip() for n in xref_row.description.split("|"))
                        )
                    identifiers.setdefault(xref_row.prefix, set()).add(
                        xref_row.identifier
                    )
                name_models = []
                for prefix, sub_names in names.items():
                    try:
                        namespace = mapping[prefix]
                    except KeyError:
                        logger.error(f"Unknown prefix '{prefix}' encountered.")
                        continue
                    name_models.extend(
                        CompoundName(name=n, namespace=namespace) for n in sub_names
                    )
                comp.names = name_models
                annotation = []
                for prefix, sub_ids in identifiers.items():
                    try:
                        namespace = mapping[prefix]
                    except KeyError:
                        logger.error(f"Unknown prefix '{prefix}' encountered.")
                        continue
                    annotation.extend(
                        CompoundAnnotation(
                            identifier=i,
                            namespace=namespace,
                            biology_qualifier=qualifier,
                        )
                        for i in sub_ids
                    )
                comp.annotation = annotation
                models.append(comp)
            session.add_all(models)
            session.commit()
            pbar.update(len(models))
    # Now we add names and identifiers for duplicated structures.
    dupes = compounds.loc[is_duplicated, :]
    with tqdm(total=len(dupes), desc="Duplicate InChI") as pbar:
        for index in range(0, len(dupes), batch_size):
            models = []
            for row in dupes.iloc[index : index + batch_size, :].itertuples(
                index=False
            ):
                logger.debug(row.mnx_id)
                comp = (
                    session.query(Compound)
                    .filter(Compound.inchi_key == row.inchi_key)
                    .one()
                )
                existing_names = {}
                for name in comp.names:
                    existing_names.setdefault(name.namespace.prefix, set()).add(
                        name.name
                    )
                existing_annotation = {}
                for identifier in comp.annotation:
                    existing_annotation.setdefault(
                        identifier.namespace.prefix, set()
                    ).add(identifier.identifier)
                # We collect names and identifiers such that we insert only
                # unique names per namespace.
                names = {}
                identifiers = {}
                # We avoid NaN (missing) values here.
                if isinstance(row.description, str):
                    names[row.prefix] = {n.strip() for n in row.description.split("|")}
                identifiers["metanetx.chemical"] = {row.mnx_id}
                identifiers.setdefault(row.prefix, set()).add(row.identifier)
                # Expand names and identifiers with cross-references.
                for xref_row in grouped_xref.get_group(row.mnx_id).itertuples(
                    index=False
                ):
                    # We avoid NaN (missing) values here.
                    if isinstance(xref_row.description, str):
                        names.setdefault(xref_row.prefix, set()).update(
                            (n.strip() for n in xref_row.description.split("|"))
                        )
                    identifiers.setdefault(xref_row.prefix, set()).add(
                        xref_row.identifier
                    )
                name_models = []
                for prefix, sub_names in names.items():
                    try:
                        namespace = mapping[prefix]
                    except KeyError:
                        logger.error(f"Unknown prefix '{prefix}' encountered.")
                        continue
                    existing = existing_names.get(prefix, frozenset())
                    name_models.extend(
                        CompoundName(name=n, namespace=namespace)
                        for n in sub_names
                        if n not in existing
                    )
                comp.names.extend(name_models)
                annotation = []
                for prefix, sub_ids in identifiers.items():
                    try:
                        namespace = mapping[prefix]
                    except KeyError:
                        logger.error(f"Unknown prefix '{prefix}' encountered.")
                        continue
                    existing = existing_annotation.get(prefix, frozenset())
                    annotation.extend(
                        CompoundAnnotation(
                            identifier=i,
                            namespace=namespace,
                            biology_qualifier=qualifier,
                        )
                        for i in sub_ids
                        if i not in existing
                    )
                comp.annotation.extend(annotation)
                models.append(comp)
            session.add_all(models)
            session.commit()
            pbar.update(len(models))
