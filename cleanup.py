from configparser import ConfigParser
from argparse import ArgumentParser
import re
import glob
import os
import time
import tarfile
from datetime import datetime
import logging
import multiprocessing
import itertools
import uuid


log_levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR
}

def multiple_file_types(*patterns):
    return itertools.chain.from_iterable(glob.iglob(pattern, recursive=True) for pattern in patterns)

def dhms(s):
    time_in_s = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
    matches = re.findall(r'(\d*[dhms])', s)
    total = 0
    for match in matches:
        for key, value in time_in_s.items():
            if key in match:
                total += int(match.strip(key)) * value
    return total

def action_remove(files: list, filter_func, dryrun=False) -> int:
    affected_files = 0
    for file in files:
        if filter_func(file):
            affected_files += 1
            if not dryrun:
                logging.debug(f'would remove {file}, but dryrun is active')
            else:
                logging.debug(f'remove {file}')
                os.remove(file)
    return affected_files


def action_echo(files: list, filter_func, dryrun=False) -> int:
    affected_files = 0
    for file in files:
        affected_files += 1
        if filter_func(file):
            logging.info(f'{file} is out of retention')
    return affected_files


def action_archive(files: list, filter_func, dryrun=False, basepath=".", name=None) -> int:
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    if not name:
        name = uuid.uuid4().hex
    output_filename = f'{basepath}/cleanup_{name}_{now}.tar.gz'
    affected_files = 0
    with tarfile.open(output_filename, "w:gz") as tar:
        for file in files:
            if filter_func(file):
                affected_files += 1
                logging.debug(f'archiving {file} into {output_filename}')
                tar.add(file, arcname=f'{os.path.dirname(file).replace(basepath, "", 1)}/{os.path.basename(file)}')
                logging.debug(f'deleting {file}')
                os.remove(file)
    if affected_files == 0:
        logging.debug(f'removing {output_filename}, it was empty')
        os.remove(output_filename)
    return affected_files


def cleanup(name: str, path: str, types: list, retention: int, action: str, dryrun=False) -> int:

    logging.info(f'cleaning up {path}')
    
    patterns = [f'{path}/**/*{type}' for type in types]
    files = multiple_file_types(*patterns)
    
    filter_func = lambda item: os.path.isfile(item) and os.path.getmtime(item) < (datetime.now().timestamp() - retention)
    
    if action == 'echo':
        return action_echo(files, filter_func)
    elif action == 'remove':
        return action_remove(files, filter_func)
    elif action == 'archive':
        return action_archive(files, filter_func, name=name)

def main():

    arg_parser = ArgumentParser()
    arg_parser.add_argument('-c', '--config', help='path to settings.ini', default='settings.ini')
    arg_parser.add_argument('-v', '--verbose', action='store_true', help='sets log level to debug')
    
    args = arg_parser.parse_args()

    settings_file = args.config

    parser = ConfigParser()
    parser.read(settings_file)

    if args.verbose:
        level = log_levels['debug']
    elif 'log_level' in parser['general']:
        level = log_levels[parser['general']['log_level'].lower()]
    else:
        level = log_levels['info']

    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(message)s')

    folders = [s for s in parser.sections() if s != 'general']

    args = list()

    for folder in folders:
        types = parser[folder]['types'].split(' ')
        retention = dhms(parser[folder]['retention'])
        action = parser[folder]['action']
        name = parser[folder].get('name')
        args.append((name, folder, types, retention, action))

    affected_files = 0

    if bool(parser['general']['threading']):
        cpu_count = multiprocessing.cpu_count()
        if len(folders) < cpu_count:
            pool_size = len(folders)
        else:
            pool_size = cpu_count
        start = time.perf_counter()    
        with multiprocessing.Pool(pool_size) as pool:
            affected_file_list = pool.starmap(cleanup, args)
        affected_files = sum(affected_file_list)
    else:
        start = time.perf_counter()
        for arg in args:
            affected_files += cleanup(**arg)

    roundtrip_time = time.perf_counter() - start
    logging.info(f'{affected_files} files affected in {roundtrip_time}s')
    

if __name__ == "__main__":
    main()