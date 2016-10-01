""" Implementation of a protocol based ont SWIM protocol described in
http://www.cs.cornell.edu/~asdas/research/dsn02-SWIM.pdf ("the paper") """
# Disable 'Too few public methods'                   pylint: disable=R0903
# Disable 'Invalid name'                             pylint: disable=C0103

class SWIM(object):
    """Class for namespacing SWIM protocol config parameters defined in
    the paper."""
    T = 2.0  # SWIM protocol period (in seconds)
    K = 3   # SWIM protocol failure detection subgroup size


