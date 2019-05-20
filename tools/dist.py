import numpy as np
from numbers import Number


def parse_dist(type_name):
    if 'uniform' == type_name:
        return UniformDistribution
    elif 'power' == type_name:
        return PowerDistribution
    elif 'zipf' == type_name:
        return ZipfDistribution
    elif 'pareto' == type_name:
        return ParetoDistribution
    elif 'truncnorm' == type_name:
        return TruncatedNormalDistribution
    return None


class ProbabilityDistribution(object):

    def __init__(self, random_state=None):
        self._random = random_state if random_state else np.random

    def generate(self):
        raise NotImplemented


class ZipfDistribution(ProbabilityDistribution):

    def __init__(self, n_ranks, a=1.01, random_state=None):
        super(ZipfDistribution, self).__init__(random_state)
        self._n_ranks = n_ranks
        self._a = a

    def generate(self):
        i = None
        while i is None:
            i = self._random.zipf(self._a)
            if i > self._n_ranks:
                i = None
        return i

    @property
    def a(self):
        return self._a

    @property
    def n_ranks(self):
        return self._n_ranks

    @n_ranks.setter
    def n_ranks(self, n_ranks):
        assert isinstance(n_ranks, Number)
        assert n_ranks > 1
        self._n_ranks = n_ranks

    @a.setter
    def a(self, a):
        assert isinstance(a, Number)
        self._a = a


class UniformDistribution(ProbabilityDistribution):

    def __init__(self, lo, hi, random_state=None):
        super(UniformDistribution, self).__init__(random_state)
        self._lo = lo
        self._hi = hi

    def generate(self):
        return self._random.uniform(self._lo, self._hi)

    @property
    def lo(self):
        return self._lo

    @property
    def hi(self):
        return self._hi


class PowerDistribution(ProbabilityDistribution):

    def __init__(self, a, n_ranks, random_state=None):
        super(PowerDistribution, self).__init__(random_state)
        self._a = a
        self._n_ranks = n_ranks
        self._probs = [1/(x ** a) for x in np.arange(1, n_ranks + 1)]
        self._probs /= sum(self._probs)

    @property
    def a(self):
        return self._a

    @property
    def n_ranks(self):
        return self._n_ranks

    def generate(self):
        return self._random.choice(np.arange(0, self._n_ranks), p=self._probs)


class ParetoDistribution(ProbabilityDistribution):

    def __init__(self, a, random_state=None):
        super(ParetoDistribution, self).__init__(random_state)
        self._a = a

    @property
    def a(self):
        return self._a

    def generate(self):
        return self._random.pareto(self._a)


class TruncatedNormalDistribution(ProbabilityDistribution):

    def __init__(self, mu, sigma, lo, hi, random_state=None):
        super(TruncatedNormalDistribution, self).__init__(random_state)
        self._mu = mu
        self._sigma = sigma
        self._lo = lo
        self._hi = hi

    def generate(self):
        i = None
        while i is None:
            i = self._random.normal(self._mu, self._sigma)
            if i > self._hi or i < self._lo:
                i = None
        return i
