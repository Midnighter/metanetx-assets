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


"""Define the CLI for generating namespace assets."""


import json
import logging
from pathlib import Path

import click
from cobra_component_models.orm import Base, Namespace
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..api import download_namespace_mapping, transform_namespaces
from ..etl import extract_namespace_mapping, extract_table, get_required_prefixes


logger = logging.getLogger(__name__)


Session = sessionmaker()


@click.group()
@click.help_option("--help", "-h")
def namespaces():
    """Subcommand for processing Identifiers.org namespaces."""
    pass


@namespaces.command()
@click.help_option("--help", "-h")
@click.argument(
    "filename", type=click.Path(exists=False, dir_okay=False, writable=True)
)
def extract_registry(filename: Path):
    """Download the Identifiers.org registry of namespaces."""
    logger.info("Extracting Identifiers.org registry...")
    mapping = download_namespace_mapping()
    logger.info("Loading...")
    with Path(filename).open(mode="w") as handle:
        # We have to convert `datetime` objects to their string representations to be
        # JSON compatible thus the argument `default=str`.
        json.dump(mapping, handle, indent=None, separators=(",", ":"), default=str)


@namespaces.command()
@click.help_option("--help", "-h")
@click.argument("db-uri", metavar="<URI>")
def reset(db_uri: str):
    """
    Reset the namespace tables.

    \b
    URI is a string interpreted as an rfc1738 compatible database URI.

    """
    logger.info("Resetting namespace tables...")
    engine = create_engine(db_uri)
    Base.metadata.drop_all(bind=engine, tables=[Namespace.__table__])
    Base.metadata.create_all(bind=engine, tables=[Namespace.__table__])


@namespaces.command()
@click.help_option("--help", "-h")
@click.argument("db-uri", metavar="<URI>")
@click.argument(
    "registry", metavar="<REGISTRY>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "chem-prop", metavar="<CHEM_PROP>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "chem-xref", metavar="<CHEM_XREF>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "comp-prop", metavar="<COMP_PROP>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "comp-xref", metavar="<COMP_XREF>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "reac-prop", metavar="<REAC_PROP>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "reac-xref", metavar="<REAC_XREF>", type=click.Path(exists=True, dir_okay=False)
)
def etl(
    db_uri: str,
    registry: click.Path,
    chem_prop: click.Path,
    chem_xref: click.Path,
    comp_prop: click.Path,
    comp_xref: click.Path,
    reac_prop: click.Path,
    reac_xref: click.Path,
):
    """
    Extract, transform, and load the namespaces used in MetaNetX.

    \b
    URI is a string interpreted as an rfc1738 compatible database URI.
    REGISTRY is a JSON file containing the Identifiers.org registry.
    CHEM_PROP is a MetaNetX table with chemical property information.
    CHEM_XREF is a MetaNetX table with chemical cross-references.
    COMP_PROP is a MetaNetX table with compartment property information.
    COMP_XREF is a MetaNetX table with compartment cross-references.
    REAC_PROP is a MetaNetX table with reaction property information.
    REAC_XREF is a MetaNetX table with reaction cross-references.

    """
    engine = create_engine(db_uri)
    session = Session(bind=engine)
    logger.info("Extracting...")
    namespace_mapping = extract_namespace_mapping(Path(registry))
    prefixes = get_required_prefixes(
        extract_table(Path(chem_prop)),
        extract_table(Path(chem_xref)),
        extract_table(Path(comp_prop)),
        extract_table(Path(comp_xref)),
        extract_table(Path(reac_prop)),
        extract_table(Path(reac_xref)),
    )
    # MetaNetX also contains EC-codes but in a separate column without prefix.
    prefixes.add("ec-code")
    logger.info("Transforming...")
    namespaces = transform_namespaces(namespace_mapping, prefixes)
    logger.info("Loading...")
    session.add_all(namespaces)
    session.commit()
