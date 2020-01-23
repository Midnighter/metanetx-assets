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


"""Define the command line interface (CLI) for generating assets."""


import json
import logging
import os
from pathlib import Path

import click
import click_log
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from cobra_component_models.orm import Base

from .api import et_namespaces, extract_prefixes, extract_namespace_mapping


logger = logging.getLogger()
click_log.basic_config(logger)
Session = sessionmaker()


try:
    NUM_PROCESSES = len(os.sched_getaffinity(0))
except OSError:
    logger.warning("Could not determine the number of cores available - assuming 1.")
    NUM_PROCESSES = 1


@click.group()
@click.help_option("--help", "-h")
@click_log.simple_verbosity_option(
    logger,
    default="INFO",
    show_default=True,
    type=click.Choice(["CRITICAL", "ERROR", "WARN", "INFO", "DEBUG"]),
)
def cli():
    """Command line interface to load the MetaNetX content into data models."""
    pass


@cli.command()
@click.help_option("--help", "-h")
@click.argument(
    "filename", type=click.Path(exists=False, dir_okay=False, writable=True)
)
def etl_namespaces(filename: Path):
    mapping = et_namespaces()
    logger.info("Loading...")
    with Path(filename).open(mode="w") as handle:
        # We have to convert `datetime` objects to their string representations to be
        # JSON compatible thus the argument `default=str`.
        json.dump(mapping, handle, indent=None, separators=(",", ":"), default=str)


@cli.command()
@click.help_option("--help", "-h")
@click.argument("db-uri", metavar="<URI>")
@click.option(
    "--drop",
    prompt="Do you *really* want to drop all existing tables in the given database?",
    default="N/y",
    help="Confirm that you want to drop all existing tables in the database.",
)
def init(db_uri, drop):
    """
    Drop any existing tables and create the SBML classes schema.

    URI is a string interpreted as an rfc1738 compatible database URI.

    """
    engine = create_engine(db_uri)
    if drop.lower().startswith("y"):
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


@cli.command()
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
def namespaces(
    db_uri: str, registry: click.Path, chem_prop: click.Path, chem_xref: click.Path,
    comp_prop: click.Path, comp_xref: click.Path,
    reac_prop: click.Path, reac_xref: click.Path,
):
    """
    Load the compartment information into a database.

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
    used_namespaces = set()
    used_namespaces.update(extract_prefixes(Path(comp_prop)))
    used_namespaces.update(extract_prefixes(Path(comp_xref)))
    used_namespaces.update(extract_prefixes(Path(comp_prop)))
    used_namespaces.update(extract_prefixes(Path(comp_xref)))
    used_namespaces.update(extract_prefixes(Path(comp_prop)))
    used_namespaces.update(extract_prefixes(Path(comp_xref)))
    logger.info("Transforming...")
    # TODO: Create ORM object of required namespaces.


@cli.command()
@click.help_option("--help", "-h")
@click.argument("db-uri", metavar="<URI>")
@click.argument(
    "registry", metavar="<REGISTRY>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "comp-prop", metavar="<COMP_PROP>", type=click.Path(exists=True, dir_okay=False)
)
@click.argument(
    "comp-xref", metavar="<COMP_XREF>", type=click.Path(exists=True, dir_okay=False)
)
def compartments(
    db_uri: str, registry: click.Path, comp_prop: click.Path, comp_xref: click.Path,
):
    """
    Load the compartment information into a database.

    URI is a string interpreted as an rfc1738 compatible database URI.
    REGISTRY is a JSON file containing the Identifiers.org registry.
    COMP_PROP is a MetaNetX table with compartment property information.
    COMP_XREF is a MetaNetX table with compartment cross-references.

    """
    engine = create_engine(db_uri)
    session = Session(bind=engine)
    logger.info("Extracting...")
    namespace_mapping = extract_namespace_mapping(Path(registry))
    comp_prop = extract_compartment_properties(Path(comp_prop))
    comp_xref = extract_compartment_cross_references(Path(comp_xref))
    logger.info("Transforming...")


@cli.command()
@click.help_option("--help", "-h")
@click.option(
    "--db-url",
    metavar="URL",
    show_default=True,
    help="A string interpreted as an rfc1738 compatible database URL.",
)
@click.option(
    "--update/--no-update",
    default=True,
    show_default=True,
    help="Check the MetaNetX FTP server for updated tables.",
)
@click.option(
    "--batch-size",
    type=int,
    default=1000,
    show_default=True,
    help="The size of batches of compounds to transform at a time.",
)
@click.argument(
    "working_dir",
    metavar="<METANETX PATH>",
    type=click.Path(exists=True, file_okay=False, writable=True),
)
@click.argument(
    "additional-compounds",
    metavar="<ADDITIONAL COMPOUNDS>",
    type=click.Path(exists=True, dir_okay=False),
)
def chemicals(
    working_dir: click.Path,
    additional_compounds: click.Path,
    db_url: str,
    update: bool,
    batch_size: int,
):
    """Drop any existing tables and populate the database using MetaNetX."""
    engine = create_engine(db_url)
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    session = Session(bind=engine)
    logger.info("Populating registries.")
    registry.populate_registries(session, chem_xref)
    logger.info("Loading compound properties.")
    chem_prop = metanetx.load_compound_properties(str(working_dir))
    logger.info("Populating compounds.")
    compounds.populate_compounds(session, chem_prop, chem_xref, batch_size)
    logger.info("Populating additional compounds.")
    compounds.populate_additional_compounds(session, additional_compounds)
    logger.info("Filling in missing InChIs from KEGG.")
    compounds.fetch_kegg_missing_inchis(session)
