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


"""Define the command line interface (CLI) for generating assets."""


import logging
import os

import click
import click_log
from cobra_component_models.orm import Base, BiologyQualifier
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .compartment import compartments
from .compound import compounds
from .namespace import namespaces
from .reaction import reactions


logger = logging.getLogger()
click_log.basic_config(logger)


Session = sessionmaker()


NUM_PROCESSES = os.cpu_count()
if NUM_PROCESSES is None:
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
    BiologyQualifier.load(Session(bind=engine))


cli.add_command(namespaces)
cli.add_command(compartments)
cli.add_command(compounds)
cli.add_command(reactions)
