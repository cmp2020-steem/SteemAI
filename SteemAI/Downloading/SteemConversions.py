from math import log10

def get_displayed_reputation(block_reputation:int) -> float:
    """
    Function to convert blockchain representation of reputation to website representation.
    Parameters
    ----------
    block_reputation : int
        The integer value for reputation stored on the blockchain
    """
    if block_reputation <= 0:
        block_reputation = 1
    reputation = 9 * (log10(block_reputation) - 9) + 25
    return reputation