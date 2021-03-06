import os
from termcolor import colored


def get_scale_factor(value_dict, max_length=os.get_terminal_size().columns):
    """
    Gets the scale factor from a dict of keys with numerical values
    """
    max_value = max(value_dict.values(), key=abs)
    try:
        scale = max_length / abs(max_value)
    except ZeroDivisionError:
        scale = 1
    return scale


def print_bars(value_dict):
    """
    Prints a bar chart to the console based on the scale factor
    """
    prefix_string = "{key}{pad}({value}): "
    align_len = get_align_len(value_dict, prefix_string)

    bar_len = os.get_terminal_size().columns - align_len
    scale_factor = get_scale_factor(value_dict, bar_len)
    for key, value in value_dict.items():
        bar_length = int(abs(value) * scale_factor)
        bar_string = '#' * bar_length
        prefix_len = len(prefix_string.format(key=key, value=value, pad=' '))
        pad_len = align_len - prefix_len
        prefix = prefix_string.format(
            key=key, value=value, pad=' ' * pad_len)
        if value > 0:
            colorized = colored(prefix + bar_string, "green")
        elif value < 0:
            colorized = colored(prefix + bar_string, "red")
        else:
            colorized = prefix + bar_string
        print(colorized)


def get_align_len(value_dict, prefix_string="{key}{pad}({value})"):
    max_len = 0
    for key, value in value_dict.items():
        length = len(prefix_string.format(key=key, value=value, pad=' '))
        if length > max_len:
            max_len = length
    return max_len
