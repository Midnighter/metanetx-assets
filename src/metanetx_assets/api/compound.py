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
    deprecated: pd.DataFrame,
    mapping: Dict[str, Namespace],
    batch_size: int = 1000,
):
    """
    Load compound information into a database.

    Parameters
    ----------
    session
    compounds
    cross_references
    deprecated
    mapping
    batch_size : int

    """
    # TODO: This is a first draft of the function. Parts should be refactored to the
    #  etl sub-package so that they can be tested better.
    # New tables include an `InChIKey=` prefix which we remove.
    compounds["inchi_key"] = compounds["inchi_key"].str[len("InChIKey=") :]
    grouped_xref = cross_references.groupby("mnx_id", sort=False)
    grouped_deprecated = deprecated.groupby("current_id", sort=False)
    # The InChI field (and thus also InChIKey) must be unique since it is the same
    # structure.
    is_duplicated = compounds.duplicated("inchi_key") & compounds["inchi_key"].notnull()
    deduped = compounds.loc[~is_duplicated, :]
    with tqdm(total=len(deduped), desc="Compound", unit_scale=True) as pbar:
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
                preferred_names = set()
                # We avoid NaN (missing) values here.
                if isinstance(row.name, str):
                    names[row.prefix] = {n.strip() for n in row.name.split("||")}
                    preferred_names.update(names[row.prefix])
                identifiers = {}
                identifiers["metanetx.chemical"] = {row.mnx_id}
                identifiers.setdefault(row.prefix, set()).add(row.identifier)
                if row.mnx_id in grouped_xref.groups:
                    # Expand names and identifiers with cross-references.
                    for xref_row in grouped_xref.get_group(row.mnx_id).itertuples(
                        index=False
                    ):
                        # We avoid NaN (missing) values here.
                        if isinstance(xref_row.description, str):
                            names.setdefault(xref_row.prefix, set()).update(
                                (n.strip() for n in xref_row.description.split("||"))
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
                    # We set preferred names from the original row description which
                    # only applies where the prefix is equal to the row's source prefix.
                    if prefix == row.prefix:
                        name_models.extend(
                            CompoundName(
                                name=n,
                                namespace=namespace,
                                is_preferred=(n in preferred_names),
                            )
                            for n in sub_names
                        )
                    else:
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
                        )
                        for i in sub_ids
                    )
                if row.mnx_id in grouped_deprecated.groups:
                    # Add deprecated MetaNetX identifiers.
                    namespace = mapping["metanetx.chemical"]
                    for depr_row in grouped_deprecated.get_group(row.mnx_id).itertuples(
                        index=False
                    ):
                        annotation.append(
                            CompoundAnnotation(
                                identifier=depr_row.deprecated_id,
                                namespace=namespace,
                                is_deprecated=True,
                            )
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
                    .join(CompoundAnnotation)
                    .join(CompoundName)
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
                if isinstance(row.name, str):
                    names[row.prefix] = {n.strip() for n in row.name.split("||")}
                identifiers["metanetx.chemical"] = {row.mnx_id}
                identifiers.setdefault(row.prefix, set()).add(row.identifier)
                if row.mnx_id in grouped_xref.groups:
                    # Expand names and identifiers with cross-references.
                    for xref_row in grouped_xref.get_group(row.mnx_id).itertuples(
                        index=False
                    ):
                        # We avoid NaN (missing) values here.
                        if isinstance(xref_row.description, str):
                            names.setdefault(xref_row.prefix, set()).update(
                                (n.strip() for n in xref_row.description.split("||"))
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
                        )
                        for i in sub_ids
                        if i not in existing
                    )
                if row.mnx_id in grouped_deprecated.groups:
                    # Add deprecated MetaNetX identifiers.
                    prefix = "metanetx.chemical"
                    namespace = mapping[prefix]
                    for depr_row in grouped_deprecated.get_group(row.mnx_id).itertuples(
                        index=False
                    ):
                        existing = existing_annotation.get(prefix, frozenset())
                        if depr_row.deprecated_id in existing:
                            continue
                        annotation.append(
                            CompoundAnnotation(
                                identifier=depr_row.deprecated_id,
                                namespace=namespace,
                                is_deprecated=True,
                            )
                        )
                comp.annotation.extend(annotation)
                session.add(comp)
                session.commit()
                pbar.update()
