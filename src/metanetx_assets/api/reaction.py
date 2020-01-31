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


"""Provide high-level reaction ETL functions."""


import logging
from typing import Dict

import pandas as pd
from cobra_component_models.orm import (
    BiologyQualifier,
    CompartmentAnnotation,
    CompoundAnnotation,
    Namespace,
    Reaction,
    ReactionAnnotation,
    ReactionName,
)
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from ..etl import EquationParser


logger = logging.getLogger()
Session = sessionmaker()


def etl_reactions(
    session: Session,
    reactions: pd.DataFrame,
    cross_references: pd.DataFrame,
    namespace_mapping: Dict[str, Namespace],
    qualifier: BiologyQualifier,
    batch_size: int = 1000,
):
    """
    Load compartment information into a database.

    Parameters
    ----------
    session
    reactions
    cross_references
    namespace_mapping
    qualifier
    batch_size : int

    """
    # TODO: This is a first draft of the function. Parts should be refactored to the
    #  etl sub-package so that they can be tested better.
    # We need to map the compound and compartment identifiers from the parsed
    # reaction equations back to their respective database rows.
    compound_mapping = {
        row.identifier: row.compound_id
        for row in session.query(
            CompoundAnnotation.identifier, CompoundAnnotation.compound_id
        )
        .filter(
            CompoundAnnotation.namespace_id == namespace_mapping["metanetx.chemical"].id
        )
        .yield_per(1000)
        # 1000 is a good value according to the documentation at
        # https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.yield_per
    }
    compartment_mapping = {
        row.identifier: row.compartment_id
        for row in session.query(
            CompartmentAnnotation.identifier, CompartmentAnnotation.compartment_id
        )
        .filter(
            CompartmentAnnotation.namespace_id
            == namespace_mapping["metanetx.compartment"].id
        )
        .yield_per(1000)
        # 1000 is a good value according to the documentation at
        # https://docs.sqlalchemy.org/en/13/orm/query.html#sqlalchemy.orm.query.Query.yield_per
    }
    grouped_xref = cross_references.groupby("mnx_id", sort=False)
    with tqdm(total=len(reactions), desc="Reaction") as pbar:
        for index in range(0, len(reactions), batch_size):
            models = []
            for row in reactions.iloc[index : index + batch_size, :].itertuples(
                index=False
            ):
                logger.debug(row.mnx_id)
                reaction = Reaction()
                reaction.participants = EquationParser.parse_participants(
                    row.equation, compound_mapping, compartment_mapping
                )
                # We collect identifiers such that we insert only unique ones per
                # namespace.
                identifiers = {}
                identifiers["metanetx.reaction"] = {row.mnx_id}
                if isinstance(row.ec_number, str):
                    identifiers["ec-code"] = {row.ec_number}
                identifiers.setdefault(row.prefix, set()).add(row.identifier)
                # Expand identifiers with cross-references.
                for xref_row in grouped_xref.get_group(row.mnx_id).itertuples(
                    index=False
                ):
                    identifiers.setdefault(xref_row.prefix, set()).add(
                        xref_row.identifier
                    )
                annotation = []
                for prefix, sub_ids in identifiers.items():
                    try:
                        namespace = namespace_mapping[prefix]
                    except KeyError:
                        logger.error(f"Unknown prefix '{prefix}' encountered.")
                        continue
                    annotation.extend(
                        ReactionAnnotation(
                            identifier=i,
                            namespace=namespace,
                            biology_qualifier=qualifier,
                        )
                        for i in sub_ids
                    )
                reaction.annotation = annotation
                models.append(reaction)
            session.add_all(models)
            session.commit()
            pbar.update(len(models))
    # TODO: Parse more names, for example, using EC-codes.
    #  Find more in etl/generate_reaction_names.py
