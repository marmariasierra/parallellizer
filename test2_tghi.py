#!/usr/bin/python

import argparse
import os
import re
import tempfile
import threading

#from collections import OrderedDict


class Reader():

    def __init__(self, number, size=0):
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


def get_data(file_name=None, folder=None):
    outputls = []

    folder = folder if folder else ""
    if file_name:
        outputls = os.popen('ghi_ls -le -f ' + file_name).read()
        outputls = outputls.split('\n')

    elif folder:
        outputls = os.popen('ghi_ls -le ' + folder).read()
        outputls = outputls.split('\n')
    else:
        #outputls = os.popen("ls " + folder).read ()
        outputls = os.popen('ghi_ls -le ' + folder).read()
        outputls = outputls.split('\n')
        #add the path tho the current directory to the filename!!# TODO add path to the files

    info = {}
    total_size_data = 0
    for line in outputls:
        # match = re.search (r'(\d+)\s\w{3}\s\d{2}\s(?:\d{2}:\d{2}|\d{4})\s+([^\s]+).+L1-TAPE:([^:]+)', line)
        # to select only files that are in hpss: use match instead of search??)
        # match = re.search(r'(^H)\s.+\s(\d+)\s\w{3}\s\d{2}\s(?:\d{2}:\d{2}|\d{4})\s+([^\s]+).+L1-TAPE:([^:]+)', line)
        match = re.match(r'(^H)\s.+\s(\d+)\s\w{3}\s\d{2}\s(?:\d{2}:\d{2}|\d{4})\s+([^\s]+).+L1-TAPE:([^:]+)', line)

        if match:
            size = int(match.group(2))
            total_size_data = total_size_data + size
            filename = match.group(3)
            tapename = match.group(4)

            if info.get(tapename):
                info[tapename]['total_size'] = info[tapename]['total_size'] + size
                info[tapename]['files'].append(filename)
            else:
                info[tapename] = {'total_size': size, 'files': [filename]}
    print(info)
    return info
    #return OrderedDict(sorted(info.items(), key=lambda x: x[1]['total_size'], reverse=True))


def init_readers(number_readers):
    readers = []
    for i in range(int(number_readers)):
        readers.append(Reader(i))
    print("{0} Total readers initiated".format(len(readers)))
    return readers


def get_next_reader(readers):
    reader_number = 0
    prev_reader_size = 0
    # readers = []

    for index, reader in enumerate(readers):
        if reader.get_size() < prev_reader_size:
            reader_number = index
        prev_reader_size = reader.get_size()
    return reader_number


def assign_readers(number_readers, file_name, folder):
    ordered_info = get_data(file_name, folder)
    readers = init_readers(number_readers)

    for tape in ordered_info.items():
        active_reader = get_next_reader(readers)
        readers[active_reader].add_file_name(tape[1].get('files'))
        readers[active_reader].add_size(tape[1].get('total_size'))
        readers[active_reader].add_tape(tape[0])

    return readers


def execute_cmd(reader):
    # ssize = float (reader.get_size ()) / 1000000000
    # print("Ssize: %d" % ssize)
    print("Reader n: {0}, Size: {1}, Tapes: {2}".format(reader.number, reader.get_size(), reader.get_tapes()))
    print("Files to read: {0}".format(reader.get_file_names()))

    with tempfile.NamedTemporaryFile() as tmp:
        filenames = reader.get_file_names()
        #tmp.write(filenames)
        tmp.writelines("%s\n" % f for f in filenames)
        #tmp.write("\n")
        tmp.seek(0)
        cmd = 'ghi_stage -v -f ' + tmp.name
        #cmd = 'ls -l ' + tmp.name
        #cmd = 'more ' + tmp.name
        print(cmd)
        out=os.system (cmd)


def main_process(number_readers, file_name, folder):
    try:
        readers = assign_readers(number_readers=number_readers, file_name=file_name, folder=folder)
        threads = []
        for reader in readers:
            threads.append(threading.Thread(target=execute_cmd, args=(reader,)))
        [thread.start() for thread in threads]

    except Exception as e:
        print("ERROR:{0}".format(e))
        raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="parallel staging")
    parser.add_argument('--readers', type=int, metavar='readers', required=True,
                        help='number of readers')
    parser.add_argument('--file', metavar='file',
                        help='file containing files to copy')
    parser.add_argument('--folder', metavar='folder',
                        help='path to directory containing files to copy')
    args = parser.parse_args()
    main_process(number_readers=args.readers, file_name=args.file, folder=args.folder)

