from pathlib import Path

import pytest

from metanetx_assets.etl import EquationParser


with (Path(__file__).parent / "data" / "sample_equations.txt").open() as handle:
    EQUATIONS = [eqn for line in handle if (eqn := line.strip())]


def test_parsing_empty_reaction() -> None:
    """"""
    assert EquationParser.parse_participants("=", {}, {}) == []


@pytest.mark.parametrize("equation", EQUATIONS)
def test_failing_equations(equation: str) -> None:
    """Test that parsing succeeds but mapping fails."""
    with pytest.raises(KeyError):
        EquationParser.parse_participants(equation, {}, {})
