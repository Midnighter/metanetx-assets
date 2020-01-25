# Copyright (c) 2020, Moritz E. Beber.
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


"""Provide specific reaction ETL functions."""


import logging
from typing import Dict, List

import pyparsing as pp
from cobra_component_models.orm import Participant


logger = logging.getLogger()


class EquationParser:
    """

    """

    compound = pp.Regex(r"MNXM\d+") | pp.Keyword("BIOMASS")
    compound.setName("compound")
    compound.__doc__ = """
    """

    compartment = (
        pp.Regex(r"MNXC\d+")
        | pp.Regex(r"MNXD\d+")
        | pp.Regex(r"MNXDX")
        | pp.Keyword("BOUNDARY")
    )
    compartment.setName("compartment")
    compartment.__doc__ = """
    """

    coefficient = pp.originalTextFor(
        pp.Word(pp.nums)
        ^ pp.Combine(pp.Word(pp.nums) + "." + pp.Word(pp.nums))
        ^ pp.nestedExpr(ignoreExpr=None)
    )
    coefficient.setName("coefficient")
    coefficient.__doc__ = """
    """

    participant = pp.Group(
        coefficient("coefficient")
        + compound("compound")
        + "@"
        + compartment("compartment")
    )
    participant.setName("participant")
    participant.__doc__ = """
    """

    reaction = (
        pp.Group(pp.delimitedList(participant, delim="+"))("substrates")
        + "="
        + pp.Group(pp.delimitedList(participant, delim="+"))("products")
    )
    reaction.setName("reaction")
    reaction.__doc__ = """
    """

    @classmethod
    def parse_participants(
        cls,
        equation: str,
        compound_mapping: Dict[str, int],
        compartment_mapping: Dict[str, int],
    ) -> List[Participant]:
        """
        Parse a reaction equation from string to ORM models.

        Parameters
        ----------
        equation
        compound_mapping
        compartment_mapping

        Returns
        -------
        list
            All the parsed reaction participants as ORM models.

        See Also
        --------
        cobra_component_models.orm.Participant

        """
        rxn = cls.reaction.parseString(equation, parseAll=True)
        result = [
            Participant(
                compound_id=compound_mapping[p.compound],
                compartment_id=compartment_mapping[p.compartment],
                stoichiometry=p.coefficient,
                is_product=False,
            )
            for p in rxn.substrates
        ]
        result.extend(
            Participant(
                compound_id=compound_mapping[p.compound],
                compartment_id=compartment_mapping[p.compartment],
                stoichiometry=p.coefficient,
                is_product=True,
            )
            for p in rxn.products
        )
        return result
