#!/usr/bin/python

import re
import os
import pprint
import argparse

from collections import OrderedDict


class Reader():

    def __init__(self, number, size):
        self.number = number
        self.size = size
        self.__tapes_to_process = []
        self.__files_to_process = []
        self.__size_stored = 0

    def add_size(self, size):
        self.__size_stored = self.__size_stored + size

    def get_size(self):
        return self.__size_stored

    def add_tape(self, tape_name):
        self.__tapes_to_process.append(tape_name)

    def get_tapes(self):
        return self.__tapes_to_process

    def add_file_name(self, filenames):
        if isinstance(filenames, str):
            self.__files_to_process.append(filenames)
        elif isinstance(filenames, list):
            self.__files_to_process = self.__files_to_process + filenames

    def get_file_names(self):
        return self.__files_to_process

    def get_full_cmd(self):
        cmd = ['ghi_stage']
        return " ".join(cmd + self.__files_to_process)



def get_data(file=None, folder=""):

    outputls=[]

    if file:
        f = open(file, 'r')
        for line in f:
            ghi_ls = os.popen('ghi_ls -le ' + line).read()
            outputls.append(ghi_ls)

    else:
        outputls = os.popen('ghi_ls -le ' + folder).read()
        outputls = outputls.split('\n')

    info = {}
    total_size_data = 0
    for line in outputls:
        match = re.search(r'(\d+)\s\w{3}\s\d{2}\s(?:\d{2}:\d{2}|\d{4})\s+([^\s]+).+L1-TAPE:([^:]+)', line)
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

    return OrderedDict(sorted(info.items(), key=lambda x: x[1]['total_size'], reverse=True)), total_size_data

def init_readers(number_readers, reader_size):
    readers = []
    for i in range(int(number_readers)):
        readers.append(Reader(i, reader_size))
    print("init_readers")
    return readers


def get_next_reader(readers):
    reader_number = 0
    prev_reader_size = 0
    # readers = []

    for index, reader in enumerate(readers):
        if reader.get_size() < prev_reader_size:
            reader_number = index
        prev_reader_size = reader.get_size()
    print("get_next")
    return reader_number

def assign_readers(number_readers, file, folder):
    try:

        ordered_info, total_size_data = get_data(file, folder)
        readers = init_readers(number_readers, reader_size=total_size_data)

        for tape in ordered_info.items():
            active_reader = get_next_reader(readers)
            readers[active_reader].add_file_name(tape[1].get('files'))
            readers[active_reader].add_size(tape[1].get('total_size'))
            readers[active_reader].add_tape(tape[0])

        for reader in readers:
            ssize = float(reader.get_size()) / 1000000000
            print("Ssize: %d" % ssize)
            print("Reader n: {0}, Size: {1}, Tapes: {2}".format(reader.number, reader.get_size(), reader.get_tapes()))
            print("Files to read: {0}".format(reader.get_file_names()))

            full_cmd = reader.get_full_cmd()
            print(full_cmd)
            os.system(full_cmd)


    except Exception as e:
        print("ERROR:{0}".format(e))
        raise e


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='assing readers')
    parser.add_argument('--readers', metavar='path', required=True,
                        help='number of readers')
    parser.add_argument('--file', metavar='path',
                        help='file containing files to copy')
    parser.add_argument('--folder', metavar='path',
                        help='path to directory containing files to copy')
    args = parser.parse_args()
    assign_readers(number_readers=args.readers, file=args.file, folder=args.folder)

