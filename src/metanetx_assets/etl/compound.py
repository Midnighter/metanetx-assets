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


"""Populate compound information."""


import asyncio
import logging
from typing import Optional, Tuple

import httpx
from cobra_component_models.orm import Compound, CompoundAnnotation, Namespace
from pandas import DataFrame, read_sql_query
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm


try:
    from openbabel import pybel
    from openbabel import openbabel as ob

    # Disable the Open Babel logging. Unfortunately, we cannot redirect the stream
    # which would be preferable.
    ob.obErrorLog.SetOutputLevel(-1)
except ModuleNotFoundError:
    import pybel


logger = logging.getLogger(__name__)


Session = sessionmaker()


def add_missing_inchi(session: Session, batch_size: int = 1000) -> None:
    """
    Retrieve InChI strings from KEGG for all compounds missing those.

    Parameters
    ----------
    session : sqlalchemy.orm.session.Session
        An active session in order to communicate with a SQL database.
    batch_size : int, optional
        The size of batches of compounds considered at a time (default 1000).

    """
    # Fetch all compounds from the database that have KEGG identifiers and are
    # missing their InChI string.
    query = (
        session.query(Compound.id, CompoundAnnotation.identifier, Compound.inchi)
        .select_from(Compound)
        .join(CompoundAnnotation)
        .join(Namespace)
        .filter(Namespace.prefix.like("kegg%"))
        .filter(Compound.inchi.is_(None))
    )
    df = read_sql_query(query.statement, session.bind)
    # The resulting data frame will contain duplicate compound primary keys.
    primary_keys = df["id"].unique()
    logger.info(
        f"There are {len(primary_keys)} KEGG compounds missing an InChI " f"string."
    )
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(collect_missing_inchi(df))
    finally:
        loop.stop()
        loop.close()
    num_inchi = len(df.loc[df["inchi"].notnull(), "id"].unique())
    logger.info(f"{num_inchi} new InChI strings were collected from KEGG.")
    grouped_df = df.groupby("id", sort=False)
    with tqdm(total=len(primary_keys), desc="KEGG Compound") as pbar:
        for index in range(0, len(primary_keys), batch_size):
            mappings = []
            for key in primary_keys.iloc[index : index + batch_size]:
                sub = grouped_df.get_group(key)
                mask = sub["inchi"].notnull()
                if mask.sum() == 0:
                    mappings.append({"id": key, "inchi": None})
                else:
                    mappings.append({"id": key, "inchi": sub.loc[mask, "inchi"].iat[0]})
            session.bulk_update_mappings(Compound, mappings)
            pbar.update(len(mappings))


async def collect_missing_inchi(
    data_frame: DataFrame, requests_per_second: int = 5
) -> None:
    """

    Parameters
    ----------
    data_frame : pandas.DataFrame
        A table of compound primary keys and KEGG identifers.
    requests_per_second : int, optional
        The rate-limit on the number of requests made per second (default 10). Not
        more than ten requests per second is what the KEGG API kindly asks you to
        observe.

    """
    tasks = []
    async with httpx.AsyncClient(
        base_url="http://rest.kegg.jp/get/",
        pool_limits=httpx.PoolLimits(hard_limit=requests_per_second),
    ) as client:
        for row in tqdm(
            data_frame.itertuples(), total=len(data_frame), desc="Submitting Request"
        ):
            tasks.append(
                asyncio.create_task(
                    fetch_inchi_from_kegg(row.Index, row.identifier, client)
                )
            )
            await asyncio.sleep(1 / requests_per_second)
    for future in tqdm(
        asyncio.as_completed(tasks), total=len(data_frame), desc="Mol to InChI"
    ):
        idx, inchi = await future
        data_frame.at[idx, "inchi"] = inchi


async def fetch_inchi_from_kegg(
    index: int, identifier: str, client: httpx.AsyncClient
) -> Tuple[int, Optional[str]]:
    """
    Fetch a single mol description of a compound from KEGG and convert it to InChI.

    Parameters
    ----------
    index : int
        The underlying data frame index.
    identifier : str
        The KEGG identifier.
    client : httpx.AsyncClient
        An httpx asynchronous client with a `base_url` set.

    Returns
    -------
    tuple
        A pair of index and InChI string or index and `None` if there was a connection
        error.

    """
    response = await client.get(f"{identifier}/mol")
    try:
        response.raise_for_status()
    except httpx.HTTPError as error:
        if response.status_code == 403:
            logger.error(
                f"{identifier}: Hit API rate limit. Please implement "
                f"exponential back off..."
            )
        else:
            logger.debug(
                f"{identifier}: Failed to fetch a molecular structure from KEGG."
                f" {str(error)}"
            )
        return index, None
    try:
        molecule = pybel.readstring("mol", response.text)
        inchi = molecule.write("inchi").strip()
    except IOError as error:
        logger.debug(f"{identifier}: {str(error)}")
        inchi = None
    if inchi:
        return index, inchi
    else:
        return index, None


def add_missing_information(session: Session, batch_size: int = 1000,) -> None:
    """
    Fill in missing structural information using openbabel.

    Parameters
    ----------
    session : sqlalchemy.orm.session.Session
        An active session in order to communicate with a SQL database.
    batch_size : int, optional
        The size of batches of compounds considered at a time (default 1000).

    """
    query = session.query(Compound).filter(Compound.inchi.isnot(None))
    num_compounds = query.count()
    for compound in tqdm(
        query.yield_per(batch_size), total=num_compounds, desc="Compound"
    ):  # type: Compound
        # If all structural data exists, we can skip this compound.
        if (
            compound.inchi_key
            and compound.smiles
            and compound.chemical_formula
            and compound.mass
            and compound.charge
        ):
            continue
        else:
            try:
                logger.debug(compound.inchi)
                molecule: pybel.Molecule = pybel.readstring("inchi", compound.inchi)
            except IOError as error:
                logger.error(
                    f"Open Babel failed to read InChI for compound " f"{compound.id}."
                )
                logger.debug("", exc_info=error)
                continue
        if not compound.inchi_key:
            compound.inchi_key = molecule.write("inchikey").strip()
        if not compound.smiles:
            compound.smiles = molecule.write("smiles").strip()
        if not compound.chemical_formula:
            compound.chemical_formula = molecule.formula
        if not compound.mass:
            compound.mass = molecule.molwt
        if not compound.charge:
            compound.charge = molecule.charge
    # Keeping the commit out of the loop here assumes that the session can keep all
    # modified objects in memory.
    session.commit()


# def populate_additional_compounds(session, filename) -> None:
#     """Populate the database with additional compounds."""
#     additional_compound_df = pd.read_csv(filename)
#     additional_compound_df[additional_compound_df.isnull()] = None
#     name_registry = session.query(Registry).filter_by(namespace="synonyms").one()
#     coco_registry = session.query(Registry).filter_by(namespace="coco").one()
#     for row in tqdm(additional_compound_df.itertuples(index=False)):
#         if session.query(exists().where(Compound.inchi == row.inchi)).scalar():
#             continue
#         logger.info(f"Adding non-MetaNetX compound: {row.name}")
#         compound = Compound(
#             mnx_id=row.mnx_id, inchi=row.inchi, inchi_key=inchi_to_inchi_key(row.inchi)
#         )
#         identifiers = []
#         if row.coco_id:
#             print(repr(row.coco_id))
#             identifiers.append(
#                 CompoundIdentifier(registry=coco_registry, accession=row.coco_id)
#             )
#         if row.name:
#             identifiers.append(
#                 CompoundIdentifier(registry=name_registry, accession=row.name)
#             )
#         compound.identifiers = identifiers
#         session.add(compound)
#     session.commit()
