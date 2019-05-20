import math
import numpy as np
from collections import Sequence
from numbers import Number
from tools.basic import returns


def parse_function(type_name):
    if 'linear' == type_name:
        return LinearFunction
    elif 'polynomial' == type_name:
        return PolynomialFunction
    elif 'constant' == type_name:
        return Constant
    elif 'exp' == type_name:
        return ExponentialFunction
    elif 'power' == type_name:
        return PowerFunction
    elif 'log' == type_name:
        return LogarithmFunction
    return None


class Function(object):

    def __init__(self):
        self._random_lo = 0.
        self._random_hi = 1.

    def randomize_params(self):
        raise NotImplemented

    def set_params_range(self, lo, hi):
        assert isinstance(lo, Number) and isinstance(hi, Number)
        assert lo < hi
        self._random_lo = lo
        self._random_hi = hi

    @property
    def random_lo(self):
        return self._random_lo

    @property
    def random_hi(self):
        return self._random_hi

    @returns(Number)
    def __call__(self, n):
        raise NotImplemented


class PolynomialFunction(Function):

    def __init__(self, degrees, coefs=None):
        super(PolynomialFunction, self).__init__()
        self.degrees = degrees
        if coefs:
            self.coefs = coefs
        else:
            self.randomize_params()

    def randomize_params(self):
        self.coefs = list(np.random.uniform(
            self.random_lo, self.random_hi, self.degrees + 1))

    @property
    def coefs(self):
        return self._coefs

    @property
    def degrees(self):
        return self._degrees

    @coefs.setter
    def coefs(self, coefs):
        assert isinstance(coefs, Sequence)
        for e in coefs:
            assert isinstance(e, Number)
        self._coefs = coefs
        self._func = np.poly1d(coefs)

    @degrees.setter
    def degrees(self, degrees):
        assert isinstance(degrees, int)
        assert degrees >= 0
        self._degrees = degrees

    @returns(Number)
    def __call__(self, n):
        assert isinstance(n, Number)
        return self._func(n)


class Constant(PolynomialFunction):

    def __init__(self, b=None):
        super(Constant, self).__init__(0, [b] if b else None)

    @property
    def b(self):
        return self._b

    @b.setter
    def b(self, b):
        assert isinstance(b, Number)
        self._b = b


class LinearFunction(PolynomialFunction):

    def __init__(self, w=None, b=None):
        if w is None or b is None:
            super(LinearFunction, self).__init__(1, None)
            self.randomize_params()
            self.coefs[0] = w if w else self.coefs[0]
            self.coefs[1] = b if b else self.coefs[1]
        else:
            super(LinearFunction, self).__init__(1, [w, b])

    @property
    def w(self):
        return self._coefs[0]

    @property
    def b(self):
        return self._coefs[1]

    @w.setter
    def w(self, w):
        assert isinstance(w, Number)
        self.coefs[0] = w

    @b.setter
    def b(self, b):
        assert isinstance(b, Number)
        self.coefs[1] = b

    def __repr__(self):
        return repr((round(self.w, 2), round(self.b, 2)))


class PowerFunction(Function):

    def __init__(self, power=None, coef=None):
        super(PowerFunction, self).__init__()
        if power is None or coef is None:
            self.randomize_params()
            self.power = power if power else self.power
            self.coef = coef if coef else self.coef
        else:
            self.power = power
            self.coef = coef

    def randomize_params(self):
        self.power = np.random.uniform(self._random_lo, self._random_hi)
        self.coef = np.random.uniform(self._random_lo, self._random_hi)

    @property
    def power(self):
        return self._power

    @property
    def coef(self):
        return self._coef

    @power.setter
    def power(self, power):
        assert isinstance(power, Number)
        self._power = power

    @coef.setter
    def coef(self, coef):
        assert isinstance(coef, Number)
        self._coef = coef

    @returns(Number)
    def __call__(self, n):
        assert isinstance(n, Number)
        return n ** self._power * self._coef


class ExponentialFunction(Function):

    def __init__(self, base=None, coef=None):
        super(ExponentialFunction, self).__init__()
        if base is None or coef is None:
            self.randomize_params()
            self.base = base if base else self.base
            self.coef = coef if coef else self.coef
        else:
            self.base = base
            self.coef = coef

    def randomize_params(self):
        self.base = np.random.uniform(self._random_lo, self._random_hi)
        self.coef = np.random.uniform(self._random_lo, self._random_hi)

    @property
    def base(self):
        return self._base

    @property
    def coef(self):
        return self._coef

    @base.setter
    def base(self, base):
        assert isinstance(base, Number)
        self._base = base

    @coef.setter
    def coef(self, coef):
        assert isinstance(coef, Number)
        self._coef = coef

    @returns(Number)
    def __call__(self, n):
        assert isinstance(n, Number)
        return self._base ** n * self._coef


class LogarithmFunction(Function):

    def __init__(self, base=None):
        super(LogarithmFunction, self).__init__()
        if base is None:
            self.randomize_params()
        else:
            self.base = base

    def randomize_params(self):
        self.base = np.random.uniform(self._random_lo, self._random_hi)

    @property
    def base(self):
        return self._base

    @base.setter
    def base(self, base):
        assert isinstance(base, Number)
        self._base = base

    @returns(Number)
    def __call__(self, n):
        assert isinstance(n, Number)
        return math.log(self._base, n)
