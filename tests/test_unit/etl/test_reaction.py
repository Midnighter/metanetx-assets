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


"""Test equation parsing for reactions."""


from pathlib import Path

import pytest

from metanetx_assets.etl import EquationParser


with (Path(__file__).parent / "data" / "sample_equations.txt").open() as handle:
    EQUATIONS = [eqn for line in handle if (eqn := line.strip())]


def test_parsing_empty_reaction() -> None:
    """Expect that an empty equation can be parsed."""
    assert EquationParser.parse_participants("=", {}, {}) == []


@pytest.mark.parametrize("equation", EQUATIONS)
def test_failing_equations(equation: str) -> None:
    """Test that parsing succeeds but mapping fails."""
    with pytest.raises(KeyError):
        EquationParser.parse_participants(equation, {}, {})
