# reduce_functions.py contains functions used by reduce.py

import random


class ReduceFunctions:

    def __init__(self, reduce_function_name):
        if reduce_function_name == 'sum_function':
            self.reduce_function = sum_function

    def get_reduce_function(self):
        return self.reduce_function


def sum_function(group, auxiliary_data):
    items = []

    total = 0
    for key, _ in group:
        total = total + 1

    item = (key, total)
    items.append(item)

    return items
