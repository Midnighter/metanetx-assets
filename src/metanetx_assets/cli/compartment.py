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


"""Define the CLI for generating compartment assets."""


import logging
from pathlib import Path

import click
from cobra_component_models.orm import (
    Base,
    BiologyQualifier,
    Compartment,
    CompartmentAnnotation,
    CompartmentName,
    Namespace,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..api import etl_compartments
from ..etl import extract_table


logger = logging.getLogger(__name__)


Session = sessionmaker()


@click.group()
@click.help_option("--help", "-h")
def compartments():
    """Subcommand for processing compartments."""
    pass


@compartments.command()
@click.help_option("--help", "-h")
@click.argument("db-uri", metavar="<URI>")
def reset(db_uri: str):
    """
    Reset the compartment tables.

    \b
    URI is a string interpreted as an rfc1738 compatible database URI.

    """
    logger.info("Resetting compartment tables...")
    engine = create_engine(db_uri)
    Base.metadata.drop_all(
        bind=engine,
        tables=[
            CompartmentAnnotation.__table__,
            CompartmentName.__table__,
            Compartment.__table__,
        ],
    )
    Base.metadata.create_all(
        bind=engine,
        tables=[
            CompartmentAnnotation.__table__,
            CompartmentName.__table__,
            Compartment.__table__,
        ],
    )


@compartments.command()
@click.help_option("--help", "-h")
@click.argument("db-uri", metavar="<URI>")
@click.argument(
    "comp-prop", metavar="<COMP_PROP>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "comp-xref", metavar="<COMP_XREF>", type=click.Path(exists=True, dir_okay=False)
)
def etl(
    db_uri: str, comp_prop: click.Path, comp_xref: click.Path,
):
    """
    Extract, transform, and load the compartments used in MetaNetX.

    \b
    URI is a string interpreted as an rfc1738 compatible database URI.
    COMP_PROP is a MetaNetX table with compartment property information.
    COMP_XREF is a MetaNetX table with compartment cross-references.

    """
    engine = create_engine(db_uri)
    session = Session(bind=engine)
    logger.info("Extracting...")
    compartments = extract_table(Path(comp_prop))
    cross_references = extract_table(Path(comp_xref))
    namespace_mapping = Namespace.get_map(session)
    qualifier_mapping = BiologyQualifier.get_map(session)
    logger.info("Transforming...")
    logger.info("Loading...")
    etl_compartments(
        session,
        compartments,
        cross_references,
        namespace_mapping,
        qualifier_mapping["is"],
    )
