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


"""Define the CLI for generating reaction assets."""


import logging
from pathlib import Path

import click
from cobra_component_models.orm import (
    Base,
    BiologyQualifier,
    Namespace,
    Reaction,
    ReactionAnnotation,
    ReactionName,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..api import etl_reactions
from ..etl import extract_table


logger = logging.getLogger(__name__)


Session = sessionmaker()


@click.group()
@click.help_option("--help", "-h")
def reactions():
    """Subcommand for processing reactions."""
    pass


@reactions.command()
@click.help_option("--help", "-h")
@click.argument("db-uri", metavar="<URI>")
def reset(db_uri: str):
    """
    Reset the reaction tables.

    \b
    URI is a string interpreted as an rfc1738 compatible database URI.

    """
    logger.info("Resetting reaction tables...")
    engine = create_engine(db_uri)
    Base.metadata.drop_all(
        bind=engine,
        tables=[
            ReactionAnnotation.__table__,
            ReactionName.__table__,
            Reaction.__table__,
        ],
    )
    Base.metadata.create_all(
        bind=engine,
        tables=[
            ReactionAnnotation.__table__,
            ReactionName.__table__,
            Reaction.__table__,
        ],
    )


@reactions.command()
@click.help_option("--help", "-h")
@click.argument("db-uri", metavar="<URI>")
@click.argument(
    "reac-prop", metavar="<REAC_PROP>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "reac-xref", metavar="<REAC_XREF>", type=click.Path(exists=True, dir_okay=False)
)
def etl(
    db_uri: str, reac_prop: click.Path, reac_xref: click.Path,
):
    """
    Extract, transform, and load the reactions used in MetaNetX.

    \b
    URI is a string interpreted as an rfc1738 compatible database URI.
    REAC_PROP is a MetaNetX table with reaction property information.
    REAC_XREF is a MetaNetX table with reaction cross-references.

    """
    engine = create_engine(db_uri)
    session = Session(bind=engine)
    logger.info("Extracting...")
    reactions = extract_table(Path(reac_prop))
    cross_references = extract_table(Path(reac_xref))
    namespace_mapping = Namespace.get_map(session)
    qualifier_mapping = BiologyQualifier.get_map(session)
    logger.info("Transforming...")
    logger.info("Loading...")
    etl_reactions(
        session,
        reactions,
        cross_references,
        namespace_mapping,
        qualifier_mapping["is"],
    )
