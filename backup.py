#!/usr/bin/env python

import sys
import os
import time
import socket
import argparse
import yaml
import shutil
import errno
from database_backup.models.mongodb_backup_info import MongodbBackupInfo
from utils import (check_is_right_node, am_i_the_right_instance, ensure_dir, tokumx_hot_backup,
                   get_directory_size, path_chown)

CONF_FILE = '/etc/dba/mongodb.yaml'
BASE_DIR = '/data'
WARNING_DAYS = 4
DELETE_DAYS = 30
BACKUP_USER = 'mongodb'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dest', default='/mfs/backup/mongodb',
                        help='Backup destination, default /mfs/backup/mongodb')
    parser.add_argument('-f', '--farm', dest='farm', metavar='FARM',
                        help='The mongo replica set name, e.g. audit/shuai/biz/alg and so on')
    parser.add_argument('-l', '--bwlimit', type=int, default=40000,
                        help='Limit backup speed(KB/s).default: 40000')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('--force-use-primary', action='store_true',
                        help='Force to backup on primary node')

    args = parser.parse_args()
    conf_dict = yaml.load(open(CONF_FILE))
    _, aliases, _ = socket.gethostbyname_ex(socket.gethostname())

    if args.farm:
        if not am_i_the_right_instance(args.farm+'-mongo', likely=1):
            if args.verbose:
                print >>sys.stderr, 'I am not "{}", exit...'.format(args.farm)
            return 1
        aliases = [args.farm+'-mongo', ]
    manually = 1 if args.farm else 0
    returncode = 0
    _returncode = 0


    for i in aliases:
        if i.find('-mongo') == -1:
            continue
        farm = i.split('-')[0]
        if farm not in conf_dict['replsets']:
            if args.verbose:
                print >>sys.stderr, 'No such mongo replica set(farm): %s.' % farm
            continue
        port = conf_dict['replsets'][farm]['port']
        user = conf_dict['common']['user']
        password = conf_dict['common']['password']
        host = socket.gethostname()
        manually = manually
        force_use_primary = 1 if args.force_use_primary else 0

        if force_use_primary and args.verbose:
            print >>sys.stdout, 'Force to backup on %s\'s primary node!' % farm
        if check_is_right_node(port, force_use_primary, manually):
            pass
        elif manually and args.verbose:
            msg = ('This is %s\'s primary node, will not do the backup job!'
                   'If you really want to bakcup on primary node, '
                   'please add --force-use-primary args.'
                   )
            print >>sys.stderr, msg % farm
            return 1
        else:
            _returncode = 1
            continue

        time_str = time.strftime('%Y%m%d%H%M%S')
        backup_dest = os.path.join(args.dest, farm, 'hot_backup', time_str)
        ensure_dir(backup_dest)

        #Before starting backup job, insert the basic infomation into database.
        info_id = MongodbBackupInfo.add(farm, None, None, host, backup_dest, 'hot_backup')
        backup_obj = MongodbBackupInfo.get(info_id)


        if not path_chown(backup_dest, BACKUP_USER):
            _returncode = 1
            continue

        if not tokumx_hot_backup(user, password, host, port, args.bwlimit,
                                 backup_dest, args.verbose):
            print >>sys.stderr, 'Hot backup on %s(%s) failed.' % (farm, host)
            _returncode = 1
            backup_obj.set_backup_success(0)
            continue
        else:
            backup_obj.set_backup_success(1)
            size = get_directory_size(backup_dest)
            backup_obj.set_backup_size_snap_used_size(size, 0)

        if _returncode > returncode:
            returncode = _returncode

        #if exec this script manually, don't do the below work(check snapshot and rm expried snapshot)
        if args.farm:
            return returncode

        #Check if exists backup for the farm_role within WARNING_DAYS days
        check = MongodbBackupInfo.check_backup_by_farm(farm, WARNING_DAYS)
        if check:
            pass
        else:
            _returncode = 1
            msg = 'There is no backup within %s days for %s'
            print >>sys.stderr, msg % (WARNING_DAYS, farm)

        #Get all dirs of backup will to be deleted
        delete_list = MongodbBackupInfo.get_backup_dir_will_delete(farm, DELETE_DAYS)
        if not delete_list:
            continue
        for del_dict in delete_list:
            try:
                shutil.rmtree(del_dict['backup_dir'])
                del_backup = MongodbBackupInfo.get(del_dict['id'])
                del_backup.set_is_delete()
            except OSError as ex:
                if ex.errno == errno.ENOENT:
                    del_backup = MongodbBackupInfo.get(del_dict['id'])
                    del_backup.set_is_delete()
                    continue
                _returncode = 1
                print >>sys.stderr, 'Remove mongodb backup %s fail!' % dir, ex
            except Exception as ex:
                _returncode = 1
                print >>sys.stderr, 'Remove mongodb backup %s fail!' % dir, ex

        if _returncode > returncode:
            returncode = _returncode

    return returncode


if __name__ == '__main__':
    sys.exit(main())

