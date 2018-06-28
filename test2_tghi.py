#!/usr/bin/python

import argparse
import os
import re
import tempfile
import threading
import logging
from dummydata import dummydata

# from collections import OrderedDict

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(threadName)s %(message)s')
logger = logging.getLogger(__name__)


class Reader:

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
    ghils_output = []

    try:

        if file_name:
            ghils_output = os.popen('ghi_ls -leu -f ' + file_name).read().split('\n')
            # ghils_output = ghils_output.split('\n')
            # catch error if file is not found: "Cannot open 'tt.txt', errno = 2"/"Could not stat 'tt.tx'. Error: 2" (absolute path needed)

        elif folder:
            ghils_output = os.popen('ghi_ls -leRu ' + folder).read()
            ghils_output = ghils_output.split('\n')
            # catch error if folder is not found: "Could not stat '/smar'. Error: 2" (absolute path needed)
        else:
            folder = os.getcwd()
            print(folder)
            ghils_output = os.popen('ghi_ls -leRu ' + folder).read()
            ghils_output = ghils_output.split('\n')

    except Exception as e:
        logger.error("ERROR:{0}".format(e))
        raise e

    ghils_output = dummydata().split('\n')

    info = {}
    # total_size_data = 0
    for line in ghils_output:
        # match = re.search (r'(\d+)\s\w{3}\s\d{2}\s(?:\d{2}:\d{2}|\d{4})\s+([^\s]+).+L1-TAPE:([^:]+)', line)
        match = re.match(r'(^H)\s.+\s(\d+)\s\w{3}\s\d{2}\s(?:\d{2}:\d{2}|\d{4})\s+([^\s]+).+L1-TAPE:([^:]+)', line)

        if match:
            size = int(match.group(2))
            # total_size_data = total_size_data + size
            filename = match.group(3)
            tapename = match.group(4)

            if info.get(tapename):
                info[tapename]['total_size'] = info[tapename]['total_size'] + size
                info[tapename]['files'].append(filename)
            else:
                info[tapename] = {'total_size': size, 'files': [filename]}
    # print(info)
    return info
    # return OrderedDict(sorted(info.items(), key=lambda x: x[1]['total_size'], reverse=True))


def init_readers(number_of_readers):
    readers = []
    for i in range(int(number_of_readers)):
        readers.append(Reader(number=i))
    print("{0} Total readers initiated".format(len(readers)))
    logger.debug("{0} Total readers initiated".format(len(readers)))
    return readers


def get_next_reader(readers):
    reader_number = 0
    prev_reader_size = 0

    for index, reader in enumerate(readers):
        if reader.get_size() < prev_reader_size:
            reader_number = index
        prev_reader_size = reader.get_size()
    return reader_number


def assign_readers(number_of_readers, file_name, folder):
    files_to_stage = get_data(file_name, folder)
    readers = init_readers(number_of_readers)

    for tape in files_to_stage.items():
        active_reader = get_next_reader(readers)
        readers[active_reader].add_file_name(tape[1].get('files'))
        readers[active_reader].add_size(tape[1].get('total_size'))
        readers[active_reader].add_tape(tape[0])
    return readers


def execute_cmd(reader):
    # ssize = float (reader.get_size ()) / 1000000000
    # print("Ssize: %d" % ssize)

    print(
        "ghi_stage_process n: {0}, Size: {1}, Tapes: {2}".format(reader.number, reader.get_size(), reader.get_tapes()))
    print("Files to stage: {0}".format(reader.get_file_names()))
    # logger.debug("Ssize: %d" % ssize)
    # logger.debug("Reader n: {0}, Size: {1}, Tapes: {2}".format(reader.number, reader.get_size(), reader.get_tapes()))
    # logger.debug("Files to read: {0}".format(reader.get_file_names()))
    # To stage files within a directory without waiting:
    # % ghi_stage -t 0 / gpfs_test_directory / stuff

    with tempfile.NamedTemporaryFile() as tmp:
        files = reader.get_file_names()
        tmp.writelines("%s\n" % f for f in files)
        tmp.seek(0)
        ghi_cmd = 'ghi_stage -v -f ' + tmp.name
        # cmd = 'ghi_stage -v -t 0 -f ' + tmp.name
        # cmd = 'ls -l ' + tmp.name
        # cmd = 'more ' + tmp.name

        try:
            outcmd = os.system(ghi_cmd)

            if outcmd > 0:
                raise Exception

        except Exception as e:
            logger.error("ERROR while executing ghi_stage: {0}, {1}, files: {2}".format(ghi_cmd, e, files))

def main_process(number_of_readers, file_name, folder):
    try:
        # validate_parameters(number_of_readers=number_of_readers, file_name=file_name, folder=folder)
        readers = assign_readers(number_of_readers=number_of_readers, file_name=file_name, folder=folder)
        threads = []
        for reader in readers:
            threads.append(threading.Thread(target=execute_cmd, args=(reader,)))
        [thread.start() for thread in threads]

    except Exception as e:
        print("ERROR:{0}".format(e))
        raise e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="parallel staging of files")
    parser.add_argument('--number', type=int, metavar='parallel processors', required=True,
                        help='number of parallel staging processes to launch. Max XXX')
    # parser.add_argument('parallel processes', type=int, metavar='parallel processes', choices=range(1,5),
    #                     help='number of parallel processes to launch. Max 4')
    # parser.add_argument('--file', metavar='file',
    #                     help='file containing files to stage, absolute path needed')
    # parser.add_argument('--folder', metavar='folder',
    #                     help='absolute path to the directory containing files to stage. Empty for current directory')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--file', metavar='file name',
                       help='file containing files to stage, absolute path needed')
    group.add_argument('--folder', metavar='folder name',
                       help='absolute path to the directory containing files to stage. Empty for current directory')
    args = parser.parse_args()

    main_process(number_of_readers=args.number, file_name=args.file, folder=args.folder)

    # Note that by default, if an optional argument is not used, the relevant variable, is given None as a value
