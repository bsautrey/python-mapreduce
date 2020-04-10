# map_functions.py contains functions used by mapper.py.

import random, string
from math import floor

import ujson

payload = string.punctuation + string.lowercase + string.uppercase
number_terabytes = 20


class MapFunctions:

    def __init__(self, map_function_name):
        if map_function_name == 'generate_keys_and_payloads':
            self.map_function = generate_keys_and_payloads

    def get_map_function(self):
        return self.map_function


# timed sort test
def generate_keys_and_payloads(line, auxiliary_data):
    items = []

    for i in range(number_terabytes):
        for j in range(1190476):  # with 10K input files, 1190476 will get you 1 TB.
            key = floor(random.uniform(0, 500000))
            item = (key, payload)
            items.append(item)

    return items
