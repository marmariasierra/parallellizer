# !/usr/bin/python

import os
import re
import math

from dummydata import dummydata
from collections import OrderedDict
from dummytest import dummytest

class Reader():

    def __init__(self, number, size_allowed):
        self.number = number
        self.__max_size_allowed = size_allowed
        self.__tapes_to_process = []
        self.__files_to_process = []
        self.__size_stored = 0
        self.__current_usage = 0

    def add_size(self, size):
        self.__size_stored = self.__size_stored + size
        self.__current_usage = int((self.__size_stored * 100) / self.__max_size_allowed)

    def get_size(self):
        return self.__size_stored

    def add_tape(self, tape_name):
        self.__tapes_to_process.append(tape_name)

    def get_tapes(self):
        return self.__tapes_to_process

    def check_size_allowed(self, size):
        return self.__size_stored + size < self.__max_size_allowed

    def add_file_name(self, filenames):
        if isinstance(filenames, str):
            self.__files_to_process.append(filenames)
        elif isinstance(filenames, list):
            self.__files_to_process = self.__files_to_process + filenames

    def get_file_names(self):
        return self.__files_to_process


    def is_not_full(self):
        return self.__size_stored < self.__max_size_allowed

    def usage(self):
        return self.__current_usage


def get_data():
    #outputls = dummydata()  # outputls = os.popen('ls -l').read()
    outputls = dummytest()  # outputls = os.popen('ls -l').read()

    info = {}
    total_size_data = 0
    for line in outputls.split("\n"):

        # match = re.search(r'(\d+)\s\w{3}\s\d{2}\s(?:\d{2}:\d{2}|\d{4})\s+./([^\s]+).+L1-TAPE:([^:]+)', line)
        match = re.search(r'(\d+)\s\w{3}\s\d{2}\s(?:\d{2}:\d{2}|\d{4})\s+./([^\s]+.tar).+L1-TAPE:([^:]+)', line)  #Only tar files
        if match:
            size = int(match.group(1))
            total_size_data = total_size_data + size
            filename = match.group(2)
            tapename = match.group(3)

            if info.get(tapename):
                info[tapename]['total_size'] = info[tapename]['total_size'] + size
                info[tapename]['files'].append(filename)
            else:
                info[tapename] = {'total_size': size, 'files': [filename]}

    # return info, total_size_data

    return OrderedDict(sorted(info.items(), key=lambda x: x[1]['total_size'], reverse=True)), total_size_data


def init_readers(number, total_size):

    allowed_reader_size = int(total_size / number)

    readers = []
    for i in range(number):
        readers.append(Reader(i, allowed_reader_size))

    return readers


def get_next_less_loaded_reader(readers):
    reader_number = 0
    prev_reader_load = 0

    for index, reader in enumerate(readers):
        if reader.get_size() < prev_reader_load:
            reader_number = index

        prev_reader_load = reader.get_size()

    return reader_number


def process(readers_number):
    try:

        ordered_info, total_size_data = get_data()

        readers = init_readers(number=readers_number, total_size=total_size_data)


        # active_reader = 0
        for tape in ordered_info.items():

            active_reader = get_next_less_loaded_reader(readers)

            readers[active_reader].add_file_name(tape[1].get('files'))
            readers[active_reader].add_size(tape[1].get('total_size'))
            readers[active_reader].add_tape(tape[0])

            # active_reader = (active_reader + 1) % 3

        for reader in readers:


            files_to_cmd = ' '.join(reader.get_file_names())
            print(files_to_cmd)
            print()

            print("Reader n: {}, Loaded: {}, Size: {}, Tapes: {}".format(reader.number, reader.usage(), reader.get_size(),reader.get_tapes()))
            print("FilesToRead: {}".format(reader.get_file_names()))

        print("Total Size: {}".format(total_size_data))

    except Exception as e:
        print("ERROR:{}".format(e))
        raise e


if __name__ == "__main__":
    readers_number = 3
    process(readers_number)


