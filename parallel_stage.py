#!/usr/bin/python

import argparse
import os
import re
import getpass
import tempfile
import threading
import logging
from dummydata import dummydata
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

path = datetime.now().strftime('logfile_%d%m%Y.log')

formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')

file_handler = TimedRotatingFileHandler(path, when="midnight", interval=1, backupCount=1)
file_handler.setFormatter(formatter)

screen_handler = logging.StreamHandler()
screen_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(screen_handler)


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

    try:

        if file_name:
            ghils_output = os.popen('ghi_ls -leu -f ' + file_name + '| sort -u ').read().split('\n')
            logger.info("Getting files from file_name: {0}, {1}".format(file_name, ghils_output))

        elif folder:
            ghils_output = os.popen('ghi_ls -leRu ' + folder).read().split('\n')
            logger.info("Getting files from folder: {0}, {1}".format(folder, ghils_output))

        else:
            folder = os.getcwd()
            ghils_output = os.popen('ghi_ls -leRu ' + folder).read().split('\n')
            logger.info("Getting files from current folder: {0}".format(ghils_output))

        if [''] in ghils_output:
            raise Exception(" No files to stage")

    except Exception as e:
        logger.error("getting data from ghi_ls command:{0}".format(e))
        raise e

    # test with local data
    ghils_output = dummydata().split('\n')

    info = {}

    for line in ghils_output:
        match = re.match(r'(^H)\s.+\s(\d+)\s\w{3}\s\d{2}\s(?:\d{2}:\d{2}|\d{4})\s+([^\s]+).+L1-TAPE:([^:]+)', line)

        if match:
            size = int(match.group(2))
            filename = match.group(3)
            tapename = match.group(4)

            if info.get(tapename):
                info[tapename]['total_size'] = info[tapename]['total_size'] + size
                info[tapename]['files'].append(filename)
            else:
                info[tapename] = {'total_size': size, 'files': [filename]}

    logger.debug("Processed info: {0}".format(info))
    return info


def init_readers(number_of_readers):
    readers = []
    for i in range(int(number_of_readers)):
        readers.append(Reader(number=i))

    logger.info("{0} Total readers initiated".format(len(readers)))
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
    # logger.debug("Ssize: %d" % ssize)
    logger.debug("ghi_stage_process n: {0}, Size: {1}, Tapes: {2}".format(reader.number, reader.get_size(), reader.get_tapes()))
    logger.debug("Files to stage: {0}".format(reader.get_file_names()))

    with tempfile.NamedTemporaryFile() as tmp:
        files = reader.get_file_names()
        tmp.writelines("%s\n" % f for f in files)
        tmp.seek(0)
        ghi_cmd = 'ghi_stage -v -f ' + tmp.name
        # ghi_cmd = 'ghi_stage -v -t 0 -f ' + tmp.name
        # cmd = 'more ' + tmp.name

        try:
            outcmd = os.system(ghi_cmd)

            if outcmd > 0:
                raise Exception

        except Exception as e:
            logger.error("while executing ghi_stage: {0}, {1}, files: {2}".format(ghi_cmd, e, files))


def main_process(number_of_readers, file_name, folder):
    try:
        readers = assign_readers(number_of_readers=number_of_readers, file_name=file_name, folder=folder)
        threads = []
        for reader in readers:
            threads.append(threading.Thread(target=execute_cmd, args=(reader,)))
        [thread.start() for thread in threads]

    except Exception as e:
        logger.error("in main process block: {0}".format(e))
        os._exit(1)


if __name__ == "__main__":

    logger.info("Stage script started by User: {0}".format(getpass.getuser()))

    parser = argparse.ArgumentParser(description="parallel staging of files")
    parser.add_argument('--number', type=int, metavar='parallel processors', required=True, choices=range(1,5),
                    help='number of parallel staging processes to launch. Max 4')
    # parser.add_argument('parallel processes', type=int, metavar='parallel processes', choices=range(1,5),
    #                     help='number of parallel processes to launch. Max 4')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--file', metavar='file name',
                       help='file containing files to stage, absolute path needed')
    group.add_argument('--folder', metavar='folder name',
                       help='absolute path to the directory containing files to stage. Empty for current directory')
    args = parser.parse_args()

    main_process(number_of_readers=args.number, file_name=args.file, folder=args.folder)

