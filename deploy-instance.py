#!/usr/bin/env python2.7

''' Utility script for deploy a MongoDb instance from scratch
'''

import os
import sys
import logging
import argparse
import subprocess
import yaml
from datetime import datetime
from utils import (ensure_dir, get_directory_size, is_space_enough,
                   find_snapshot, get_mongodb_info, run_instance_deploy)

CONF_FILE = '/etc/dba/mongodb.yaml'
SPACE_TO_KEEP = 50  # GB
BACKUP_DIR = '/mfs/backup/mongodb'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--docker-image',
                        default='dba-registry:5000/tokumx-2.0:online',
                        help='which docker image to use. default: dba-registry:5000/tokumx-2.0:online')
    parser.add_argument('-f', '--farm', required=True,
                        help='MongoDB replSet name.'
                        )
    parser.add_argument('--rsync-data',
                        action='store_true',
                        help='really need to rsync backup data(source) to this server(destination)? default: False')
    parser.add_argument('--bwlimit', type=int, default=30000,
                        help='default: 30000')
    parser.add_argument('-s', '--source',
                        help='Which backup set to use (full path is needed)')
    parser.add_argument('-l', '--use-latest-snapshot', action='store_true',
                        help='Use latest backupSet')
    parser.add_argument('--snapshot-store',
                        default='/mfs/backup/mongodb')

    parser.add_argument('--data-root', default='/data',
                        help='default: /data')
    parser.add_argument('--log-root', default='/data',
                        help='default: /data')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    args = parser.parse_args()

    if args.verbose >= 2:
        level = logging.DEBUG
    elif args.verbose == 1:
        level = logging.INFO
    else:
        level = logging.WARNING
    basic_format = '%(asctime)s %(levelname)s [%(name)s] %(message)s'
    logging.basicConfig(format=basic_format, level=level)
    logger = logging.getLogger(__name__)

    conf_dict = yaml.load(open(CONF_FILE))
    if args.farm not in conf_dict['replsets']:
        if args.verbose:
            print >>sys.stderr, 'No such MongoDB replica set(farm): %s.' % (args.farm)
        return 1

    farm_dir = 'mongo-{}'.format(args.farm)
    data_dir = os.path.join(args.data_root, farm_dir)
    log_dir = os.path.join(args.log_root, 'mongo-{}-log'.format(args.farm))
    ensure_dir(data_dir)
    ensure_dir(log_dir)

    if args.rsync_data:
        if args.use_latest_snapshot:
            source = find_snapshot(args.snapshot_store, args.farm, datetime.now())
            if not source:
                logger.debug('No snapshot found for "%s" farm under %s',
                             args.farm, args.snapshot_store)
                return 1
        else:
            source = args.source
        if not source:
            logger.error(('either --source or --use-latest-snapshot should be provided'))
            return 1
        if not os.path.isdir(source):
            logger.error('%s as a "snapshot" shoud be a directory', source)
            return 1

        rsync = ['rsync', '-a', '--drop-cache', '--bwlimit', str(args.bwlimit)]
        data_size = get_directory_size(source)

        if not is_space_enough(args.data_root, data_size, SPACE_TO_KEEP):
            logger.error('%sGB is needed, there is no enough space on %s', data_size, args.data_root)
            return 1
        # rsync the data
        source = source.rstrip('/') + '/'
        cmd = rsync + [source, data_dir]
        proc = subprocess.Popen(cmd)
        proc.wait()

    user = conf_dict['common']['user']
    password = conf_dict['common']['password']
    port = conf_dict['replsets'][args.farm]['port']
    host = '%s-mongo' % (args.farm)
    key = conf_dict['replsets'][args.farm]['key']

    for i in range(1,4):
        mongodb_info = get_mongodb_info(host+str(i), port, user, password)
        if mongodb_info:
            break
    if not mongodb_info.get('maxConns') or not mongodb_info.get('cacheSize'):
        if args.verbose:
            msg = ('May be not any %s node Available.'
                   'The value of maxConns and cacheSize must be get.'
                   ) % (args.farm)
            print >>sys.stderr, msg
        return 1

    logger.debug('%r', mongodb_info)
    return run_instance_deploy(args.docker_image, args.farm, port, data_dir,
                               log_dir, BACKUP_DIR, key, **mongodb_info)


if __name__ == '__main__':
    sys.exit(main())

# vim: set et sw=4 ts=4 nu :
