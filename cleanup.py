from configparser import ConfigParser
from argparse import ArgumentParser
import re
import glob
import os
import shutil
import time
import tarfile
from datetime import datetime
import logging
import multiprocessing


log_levels = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR
}


def dhms(s):
    time_in_s = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
    matches = re.findall(r'(\d*[dhms])', s)
    total = 0
    for match in matches:
        for key, value in time_in_s.items():
            if key in match:
                total += int(match.strip(key)) * value
    return total

def cleanup(path, type, globs, retention, action, dryrun=False):

    affected_files = 0

    for gl in globs:
        
        check_time = time.time() - retention
        now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        
        g = glob.glob(f'{path}/**/{gl}', recursive=True)

        if action != 'archive' or dryrun:
            for file in g:

                if type in ['f', 'file']:
                    type_check = os.path.isfile(file)
                elif type in ['d', 'dir', 'directory', 'folder']:
                    type_check = os.path.isdir(file)
                else:
                    type_check = False

                if os.path.exists(file) and type_check and os.path.getmtime(file) < check_time:
                    if action == 'print' or dryrun:
                        logging.info(file)
                    elif action == 'delete':
                        logging.debug(f'deleting {file}')
                        if type in ['f', 'file']:
                            os.remove(file)
                        else:
                            shutil.rmtree(file)
                    affected_files += 1

        elif action == 'archive':
            output_filename = f'{path}/cleanup_{type}_{now}.tar.gz'
            if len(g) > 0:
                with tarfile.open(output_filename, "w:gz") as tar:
                    for file in g:

                        if type in ['f', 'file']:
                            type_check = os.path.isfile(file)
                        elif type in ['d', 'dir', 'directory']:
                            type_check = os.path.isdir(file)
                        else:
                            type_check = False

                        if os.path.exists(file) and type_check and os.path.getmtime(file) < check_time:
                            logging.debug(f'archiving {file} into {output_filename}')
                            tar.add(file, arcname=os.path.basename(file))
                            affected_files += 1
                            
                    for file in g:
                        if os.path.exists(file) and type_check and os.path.getmtime(file) < check_time:
                            logging.debug(f'deleting {file}')
                            if type in ['f', 'file']:
                                os.remove(file)
                            else:
                                shutil.rmtree(file)
    
    return affected_files
    

def main():

    arg_parser = ArgumentParser()
    arg_parser.add_argument('-c', '--config', help='path to settings.ini')
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

    logging.basicConfig(level=level)

    folders = [s for s in parser.sections() if s != 'general']

    args = list()

    for folder in folders:
        type = parser[folder]['type']
        globs = parser[folder]['globs'].split(' ')
        retention = dhms(parser[folder]['retention'])
        action = parser[folder]['action']
        args.append((folder, type, globs, retention, action))

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