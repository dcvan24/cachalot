import hashlib
import functools


def sha1_encode(*args):
    def encode(v):
        return repr(v).encode('utf-8')
    base = hashlib.sha1()
    for arg in args:
        if type(arg) in (list, set):
            for i in arg:
                base.update(encode(sha1_encode(i)))
        elif isinstance(arg, dict):
            for k, v in arg.items():
                base.update(encode(sha1_encode(k)))
                base.update(encode(sha1_encode(v)))
        elif type(arg) in (str, int, float, bool) or arg is None:
            base.update(encode(arg))
        else:
            base.update(encode(hash(arg)))
    return int(base.hexdigest(), 16)


def lazy_property(function):
    attribute = '_' + function.__name__

    @property
    @functools.wraps(function)
    def wrapper(self):
        if not hasattr(self, attribute):
            setattr(self, attribute, function(self))
        return getattr(self, attribute)
    return wrapper


def returns(*accepted_return_type_tuple):
    def return_decorator(validate_function):
        assert len(accepted_return_type_tuple) > 0

        @functools.wraps(validate_function)
        def decorator_wrapper(*function_args):
            assert len(accepted_return_type_tuple) == 1
            accepted_return_type = accepted_return_type_tuple[0]
            return_value = validate_function(*function_args)
            assert isinstance(return_value, accepted_return_type)
            return return_value

        return decorator_wrapper
    return return_decorator
