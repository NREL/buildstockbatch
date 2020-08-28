import traceback
import inspect


def _str_repr(obj, list_max=20, dict_max=20, string_max=100):
    if type(obj) is str:
        if len(obj) <= string_max:
            return f"'{obj}'"
        else:
            return f"'{obj[0:string_max//2]}...{len(obj)}...{obj[-string_max//2:]}'"
    elif type(obj) in [int, float]:
        return _str_repr(str(obj), list_max, dict_max, string_max)
    elif type(obj) is list:
        txt = "[" + ",".join([_str_repr(item, list_max, dict_max, string_max) for item in obj[0:list_max]])
        if len(obj) > list_max:
            txt += f" ...{len(obj)}"
        txt += "]"
        return txt
    elif type(obj) is tuple:
        txt = "(" + ",".join([_str_repr(item, list_max, dict_max, string_max) for item in obj[0:list_max]])
        if len(obj) > list_max:
            txt += f" ...{len(obj)}"
        txt += ")"
        return txt
    elif type(obj) is set:
        obj = list(obj)
        txt = "{" + ",".join([_str_repr(item, list_max, dict_max, string_max) for item in obj[0:dict_max]])
        if len(obj) > dict_max:
            txt += f" ...{len(obj)}"
        txt += "}"
        return txt
    elif type(obj) is dict:
        keys = list(obj.keys())
        txt = "{" + ",".join([f"{_str_repr(key, list_max, dict_max, string_max)}:"
                              f" {_str_repr(obj[key], list_max, dict_max, string_max)}" for key in keys[0:dict_max]])
        if len(keys) > dict_max:
            txt += f" ...{len(keys)}"
        txt += "}"
        return txt
    else:
        return str(obj)


def get_error_details():
    text = ""
    text += traceback.format_exc()
    frames = inspect.trace()
    for frame in frames:
        text += f'\nIn file: {frame[1]}, module {str(frame[3])} line: {frame[2]} \n'
        text += "Local Variables: "
        for var, value in frame[0].f_locals.items():
            text += _str_repr(var) + ":" + _str_repr(value)
            text += "\n"
    return text


def log_error_details(output_file="buildstockbatch_crash_details.log"):
    def log_error_decorator(func):
        def run_with_error_capture(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                with open(output_file, "a") as f:
                    text = "\n" + "#"*20 + "\n"
                    text += get_error_details()
                    f.write(text)
                raise
        return run_with_error_capture

    return log_error_decorator
