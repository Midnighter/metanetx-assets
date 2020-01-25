# Copyright (c) 2019, Novo Nordisk Foundation Center for Biosustainability,
# Technical University of Denmark.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Generate reaction names.

Reads all reaction identifiers from the metanetx source files, and tries to look
up the common names for the reaction from BiGG, kegg, modelseed or EC.
"""

import csv
import json
import logging
import logging.config
import re
from collections import defaultdict

import requests
from tqdm import tqdm

from metanetx import data


logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "simple": {
                "format": "%(asctime)s %(name)s::%(funcName)s:%(lineno)d %(message)s"
            }
        },
        "handlers": {
            "console": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "simple",
            }
        },
        "loggers": {
            "urllib3.connectionpool": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "simple",
            }
        },
        "root": {"level": "DEBUG", "handlers": ["console"]},
    }
)
logger = logging.getLogger(__name__)

logger.info("Downloading ModelSEED reaction database")
# ModelSEED ID -> ModelSEED name
model_seed_map = {}
response = requests.get(
    "https://raw.githubusercontent.com/ModelSEED/ModelSEEDDatabase/dev/Biochemistry/reactions.tsv",
    stream=True,
)
response.raise_for_status()
for row in csv.DictReader(response.iter_lines(decode_unicode=True), delimiter="\t"):
    model_seed_map[row["id"]] = row["name"]
logger.info(f"Mapped {len(model_seed_map)} ModelSEED reactions")

logger.info("Reading metanetx source files")
data.load_metanetx_data()


def lookup_bigg(bigg_id):
    response = requests.get(
        f"http://bigg.ucsd.edu/api/v2/universal/reactions/{bigg_id}"
    )
    response.raise_for_status()
    return response.json().get("name")


def lookup_seed(seed_id):
    return model_seed_map[seed_id]


def lookup_kegg(kegg_id):
    response = requests.get(f"http://rest.kegg.jp/find/reaction/{kegg_id}")
    response.raise_for_status()
    line = response.text
    # The line format wasn't well documented, splitting on tab and semicolon was
    # just found by inspecting a couple of examples. Might not hold up
    # perfectly.
    return line.split("\t", 1)[1].split(";", 1)[0]


def lookup_ec(ec):
    # Try to look up name based on EC number
    # Only try if the EC numbers is exact (4 numbers), and there is only a
    # single one.
    regex = r"^\d+\.\d+\.\d+\.\d+$"
    if re.match(regex, ec):
        response = requests.get(f"https://enzyme.expasy.org/EC/{ec}.txt")
        response.raise_for_status()
        for line in response.text.split("\n"):
            key, value = line.split(maxsplit=1)
            if key == "DE":
                return value


# metanetx id -> resolved name
logger.info(f"Mapping {len(data.reactions)} reactions, this will take some time.")
name_map = {}
unmapped = []
stats = defaultdict(int)
exceptions = []
for reaction in tqdm(data.reactions.values()):
    try:
        name = None
        if "bigg" in reaction.annotation:
            name = lookup_bigg(reaction.annotation["bigg"][0])
            stats["bigg"] += 1
        if not name and "seed" in reaction.annotation:
            name = lookup_seed(reaction.annotation["seed"][0])
            stats["seed"] += 1
        if not name and "kegg" in reaction.annotation:
            name = lookup_kegg(reaction.annotation["kegg"][0])
            stats["kegg"] += 1
        if not name and reaction.ec != "":
            name = lookup_ec(reaction.ec)
            stats["ec"] += 1
        if name:
            name_map[reaction.mnx_id] = name
        else:
            unmapped.append(reaction.mnx_id)
    except Exception as e:
        exceptions.append(f"{reaction.mnx_id}: {str(e)}")

with open("data/reaction_names.json", "w") as f:
    json.dump(name_map, f)

with open("data/reaction_names_unmapped.json", "w") as f:
    json.dump(unmapped, f)

with open("data/exceptions.json", "w") as f:
    json.dump(exceptions, f)

print(f"Total: {len(name_map)} reactions mapped, {len(unmapped)} unmapped")
for k, v in stats.items():
    print(f"  {k}: {v} reactions found")
print(
    "Mapped reactions stored in `data/reaction_names.json` (please commit this to git)"
)
print(
    "Unmapped reactions stored in `data/unmapped-reactions.json` for temporary inspection, delete it when done"
)
print(
    f"{len(exceptions)} exceptions occurred, stored in `data/exceptions.json` for temporary inspection, delete it when done"
)
