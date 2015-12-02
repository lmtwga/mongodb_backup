import os
import socket
import sys
import pymongo
import re
import subprocess
import pwd
import time
import grp
import glob
import math
import logging
import docker
from docker.errors import APIError
from docker.constants import DEFAULT_DOCKER_API_VERSION


def ensure_dir(path):
    # make sure / and path not belong to the same device
    if os.path.exists(path):
        if os.stat("/").st_dev == os.stat(path).st_dev:
            raise Exception("root / and %s should not belong to the same device" % path)
        return

    parent = os.path.dirname(path)
    if parent == "/":
        raise Exception("will not mkdir %s, the parent dir is /" % path)
    if not os.path.exists(parent):
        ensure_dir(parent)
    os.mkdir(path)


def convert_to_gb(size):
    return float(size) / (1024 ** 3)


def get_directory_size(path):
    ''' Calculate directory size
    '''

    size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            fullpath = os.path.join(dirpath, filename)
            stat = os.stat(fullpath)
            size += stat.st_size
    return convert_to_gb(size)


def path_chown(path, username, groupname=''):
    if not groupname:
        groupname = username
    ensure_dir(path)
    try:
        uid = pwd.getpwnam(username).pw_uid
    except KeyError:
        print >>sys.stderr, 'User %s does not exist.' % username
        return False
    try:
        gid = grp.getgrnam(groupname).gr_gid
    except KeyError:
        print >>sys.stderr, 'Group %s does not exist.' % groupname
        return False
    os.chown(path, uid, gid)
    return True


def am_i_the_right_instance(name, likely=0):
    try:
        _, aliases, _ =socket.gethostbyname_ex(socket.gethostname())
        if not likely:
            return name in aliases
        else:
            for i in aliases:
                if i.startswith(name):
                    return True
        return False
    except:
        return False


def check_is_right_node(port, force_use_primary, manually, node_nums=3):
    '''
    Check the given hostname is the right node to backup.
    If manually=1, any secondary node can be used as backup node.
    If manually=0, only this one secondary node can be used as backup node:
        if primary is 'farm-mongoN', the backup node is 'farm-mongo'+str(N%3+1).
        e.g. primary='biz-mongo1', the backup node is 'biz-mongo2';
            primary='biz-mongo2', the backup node is 'biz-mongo3';
            primary='biz-mongo3', the backup node is 'biz-mongo1';
    If force_use_primary=1, Then any node can be used as backup node.
    '''

    db_host_port = '{0}:{1}'.format('localhost', port)
    mongo_client = pymongo.MongoClient(db_host_port)
    try:
        master_dict = dict(mongo_client.admin.command("isMaster"))
    except Exception as exc:
        print >>sys.stderr, 'Connect mongo: %s failed.' % (db_host_port,), exc
        return False
    mongo_me = master_dict['me'].split(':')[0]
    mongo_primary = master_dict['primary'].split(':')[0]
    target = mongo_primary[0:-1] + str(int(mongo_primary[-1:]) % node_nums +1)

    if force_use_primary:
        return True
    if manually:
        if mongo_me != mongo_primary:
            return True
        else:
            return False
    else:
        if mongo_me == target:
            return True
        else:
            return False


def tokumx_hot_backup(user, password, host, port, bwlimit, backup_dest, verbose):
    try:
        url = 'mongodb://%s:%s@%s:%s' % (user, password, host, port)
        conn = pymongo.Connection(url)
        db = pymongo.database.Database(conn,'admin')
        backupstatus = db.command("backupStatus", check=False)
        if backupstatus.get('percent'):
            if verbose:
                print >>sys.stderr, 'Another backup job is in progress!'
            return False

        db.command({'backupThrottle': str(bwlimit)+'KB'})
        db.command({"backupStart": backup_dest})
    except Exception as exc:
        if verbose:
            print >>sys.stderr, 'Backup failed.', exc
        return False
    return True


def lvm_snap(backup_opt_dict):
    # Create the snapshot according the values passed from command line
    lvm_create_snap = '{0} --size {1} --snapshot --name {2} {3}'.format(
        backup_opt_dict['lvcreate_path'],
        str(backup_opt_dict['lvm_snapsize'])+'G',
        backup_opt_dict['lvm_snapname'],
        backup_opt_dict['lvm_srcvol'])
    if backup_opt_dict['verbose']:
        print >>sys.stdout, lvm_create_snap
    lvm_process = subprocess.Popen(
        lvm_create_snap, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, shell=True,
        executable=backup_opt_dict['bash_path'])
    lvm_out, lvm_err = lvm_process.communicate()
    rc = lvm_process.returncode
    if rc != 0:
        if backup_opt_dict['verbose']:
            print >>sys.stderr, 'lvm snapshot creation error: {0}'.format(lvm_err)
        return False
    elif backup_opt_dict['verbose']:
        print >>sys.stdout, lvm_out
    else:
        pass

    # Mount the newly created snapshot to dir_mount
    abs_snap_name = '/dev/{0}/{1}'.format(
        backup_opt_dict['lvm_volgroup'],
        backup_opt_dict['lvm_snapname'])
    mount_snap = '{0} {1} {2} {3}'.format(
        backup_opt_dict['mount_path'],
        backup_opt_dict['mount_options'],
        abs_snap_name,
        backup_opt_dict['lvm_dirmount'])
    if backup_opt_dict['verbose']:
        print >>sys.stdout, mount_snap
    mount_process = subprocess.Popen(
        mount_snap, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, shell=True,
        executable=backup_opt_dict['bash_path'])
    mount_out, mount_err = mount_process.communicate()
    rc = mount_process.returncode
    if 'already mounted' in mount_err:
        if backup_opt_dict['verbose']:
            print >>sys.stderr, 'Volume {0} already mounted on {1}'.format(
                abs_snap_name, backup_opt_dict['lvm_dirmount'])
        return False
    if rc !=0:
        if backup_opt_dict['verbose']:
            print >>sys.stderr, 'lvm snapshot mounting error: {0}'.format(mount_err)
        return False
    else:
        if backup_opt_dict['verbose']:
            print >>sys.stderr, 'Volume {0} succesfully mounted on {1}'.format(
                abs_snap_name, backup_opt_dict['lvm_dirmount'])
    return True


def lvm_snap_remove(backup_opt_dict):
    mapper_snap_vol = '/dev/mapper/{0}-{1}'.format(
        backup_opt_dict['lvm_volgroup'],
        backup_opt_dict['lvm_snapname'])
    try:
        with open('/proc/mounts', 'r') as proc_mount_fd:
            for mount_line in proc_mount_fd:
                if mapper_snap_vol.lower() in mount_line.lower():
                    dev_vol, mount_point = mount_line.split(' ')[:2]
                    umount_cmd = '{0} -l -f {1}'.format(backup_opt_dict['umount_path'], mount_point)
                    if backup_opt_dict['verbose']:
                        print >>sys.stdout, umount_cmd
                    umount_proc = subprocess.Popen(
                        umount_cmd,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        shell=True, executable=backup_opt_dict['bash_path'])
                    umount_err = umount_proc.communicate()
                    rc = umount_proc.returncode
                    if rc != 0:
                        if backup_opt_dict['verbose']:
                            mfg = 'impossible to umount {0}. {1}'.format(mount_point, umount_err)
                            print >>sys.stderr, mfg
                        return False

                    # Change working directory to be able to unmount
                    os.chdir(backup_opt_dict['workdir'])
                    snap_rm_cmd = '{0} -f {1}'.format(
                        backup_opt_dict['lvremove_path'],
                        mapper_snap_vol)
                    if backup_opt_dict['verbose']:
                        print >>sys.stdout, snap_rm_cmd
                    snap_rm_proc = subprocess.Popen(
                        snap_rm_cmd,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        shell=True, executable=backup_opt_dict['bash_path'])
                    lvm_rm_out, lvm_rm_err = snap_rm_proc.communicate()
                    if 'successfully removed' in lvm_rm_out:
                        if backup_opt_dict['verbose']:
                            print >>sys.stdout, lvm_rm_out
                        return True
                    else:
                        if backup_opt_dict['verbose']:
                            print >>sys.stderr, 'lvm_snap_rm {0}'.format(lvm_rm_err)
                        return False
    except:
        return False
    return False


def get_lvm_info(backup_opt_dict):
    mount_point_path = backup_opt_dict['mount_point_path']
    try:
        with open('/proc/mounts', 'r') as mount_fd:
            for mount_line in mount_fd:
                device, mount_path = mount_line.split(' ')[0:2]
                if mount_point_path.strip() == mount_path.strip():
                    mount_match = re.search(r'/dev/mapper/(\w.+?\w)-(\w.+?\w)$', device)
                    if mount_match:
                        backup_opt_dict['lvm_volgroup'] = mount_match.group(1)
                        backup_opt_dict['lvm_srcvol'] = u'/dev/{0}/{1}'.format(
                            backup_opt_dict['lvm_volgroup'], mount_match.group(2))
                        break
    except:
        return False
    return backup_opt_dict


def get_vg_free_space(backup_opt_dict):
    free_cmd = '{0} --aligned -o vg_free --noheadings --units g --nosuffix {1}'.format(
        backup_opt_dict['vgs_path'],
        backup_opt_dict['lvm_volgroup'])
    if backup_opt_dict['verbose']:
        print >>sys.stdout, free_cmd
    lvm_process = subprocess.Popen(
        free_cmd, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, shell=True,
        executable=backup_opt_dict['bash_path'])
    lvm_out, lvm_err = lvm_process.communicate()
    rc = lvm_process.returncode
    if rc != 0:
        if backup_opt_dict['verbose']:
            print >>sys.stderr, 'Get the vg free space of {} fail!'.format(
                backup_opt_dict['lvm_volgroup']), lvm_err
        return False
    else:
        backup_opt_dict['vg_free_size'] = float(lvm_out.strip())
        return backup_opt_dict


def get_snap_used_size(backup_opt_dict):
    mapper_snap_vol = '/dev/mapper/{0}-{1}'.format(
        backup_opt_dict['lvm_volgroup'],
        backup_opt_dict['lvm_snapname'])
    free_cmd = ('{0} --aligned -o snap_percent,lv_size --noheadings --units g --nosuffix {1}'
                ).format(backup_opt_dict['lvs_path'], mapper_snap_vol)
    if backup_opt_dict['verbose']:
        print >>sys.stdout, free_cmd
    lvm_process = subprocess.Popen(
        free_cmd, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, shell=True,
        executable=backup_opt_dict['bash_path'])
    lvm_out, lvm_err = lvm_process.communicate()
    rc = lvm_process.returncode
    if rc != 0:
        if backup_opt_dict['verbose']:
            print >>sys.stderr, 'Get the snapshot of {} used size fail!'.format(
                mapper_snap_vol), lvm_err
        return False
    else:
        snap_percent, lv_size = lvm_out.strip().split()
        backup_opt_dict['snap_used_size'] = round(float(snap_percent) * float(lv_size)/100,2)
    return backup_opt_dict


def rsync_backup(backup_opt_dict):
    data_dir = os.path.join(backup_opt_dict['lvm_dirmount'],
                            'mongo-{}'.format(backup_opt_dict['farm']))
    rsync_cmd = '{0} --drop-cache --bwlimit {1} {2} {3} {4}'.format(
        backup_opt_dict['rsync_path'],
        backup_opt_dict['bwlimit'],
        '-avP' if backup_opt_dict['verbose'] else '-a',
        data_dir,
        backup_opt_dict['backup_dest'])
    if backup_opt_dict['verbose']:
        print >>sys.stdout, rsync_cmd
    proc = subprocess.Popen(rsync_cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, shell=True,
                            executable=backup_opt_dict['bash_path'])
    if backup_opt_dict['verbose']:
        for line in iter(proc.stdout.readline, ''):
            print >>sys.stdout, line
    rsync_out, rsync_err = proc.communicate()
    rc = proc.returncode
    if rc != 0:
        if backup_opt_dict['verbose']:
            msg = 'Rsync from {0} to {1} fail!'.format(data_dir,
                                                       backup_opt_dict['backup_dest'])
            print >>sys.stderr, msg, rsync_err
        return False
    return True


def get_mongodb_info(host, port, user, password):
    url = 'mongodb://{0}:{1}@{2}:{3}'.format(user, password, host, port)
    mongo_client = pymongo.MongoClient(url)
    try:
        server_status = mongo_client.admin.command("serverStatus")
        con_used = server_status.get('connections').get('current')
        con_unuse = server_status.get('connections').get('available')
        cache_size = server_status.get('ft').get('cachetable').get('size').get('limit')
    except Exception as exc:
        print >>sys.stderr, 'Connect MongoDB: %s:%s failed.' % (host, port), exc
        return 0
    mongodb_info = {}
    mongodb_info['maxConns'] = int(con_used + con_unuse)
    mongodb_info['cacheSize'] = int(cache_size)
    return mongodb_info


def get_api_version(base_url='unix://var/run/docker.sock'):
    client = docker.Client(base_url=base_url, version='1.0')
    try:
        info = client.version()
        return info['ApiVersion']
    except APIError as ae:
        import re
        rs = re.findall('server:\s((\d+\.?)+)', str(ae))
        if rs:
            return rs[0][0]
    return DEFAULT_DOCKER_API_VERSION


def get_client(base_url='unix://var/run/docker.sock'):
    api_version = get_api_version(base_url)
    return docker.Client(base_url=base_url, version=api_version)


def find_snapshot(snapshot_store, farm, before):
    ''' Find newest snapshot before time `before`

    Directory store of the snapshot store:
    `snapshot_store`
      - `farm`
        - snapshots
          - %Y%m%d%H%M%S
    '''

    farm_store = os.path.join(snapshot_store, farm, 'hot_backup')
    slot = os.path.join(farm_store, before.strftime('%Y%m%d%H%M%S'))
    pattern = os.path.join(farm_store, '*')
    snapshots = sorted(glob.glob(pattern) + [slot])
    index = snapshots.index(slot)
    return None if index == 0 else snapshots[index-1]


def is_space_enough(path, needs, space_to_keep):
    ''' Check if there is enough space to meet the needs(in GB)
    '''

    s = os.statvfs(os.path.realpath(path))
    avail = int(math.floor(float(s.f_bfree * s.f_bsize) / (1024 ** 3)))
    logging.info('%s avail: %s, needs: %s', path, avail, needs)
    return avail > (space_to_keep + needs)


def _build_binds(data_dir, log_dir, backup_dir):
    src_dst = []
    src_dst.append((data_dir, '/mongodb/data'))
    src_dst.append((log_dir, '/mongodb/logs'))
    src_dst.append((backup_dir, backup_dir))
    binds = {}
    for src, dst in src_dst:
        binds[src] = {
            'bind': dst,
            'ro': False,
        }
    return binds


def is_container_running(container, base_url='unix://var/run/docker.sock'):
    ''' Check if is container running
    '''

    logger = logging.getLogger(__name__)

    client = get_client(base_url)
    try:
        info = client.inspect_container(container)
        return info['State']['Running']
    except docker.errors.APIError, ae:
        if 'No such container' in ae.explanation:
            msg = 'container {} does not exist'.format(container)
            logger.debug(msg)
            return False
        else:
            msg = 'failed to inspect container {}'.format(container)
            logger.exception(msg)
            raise


def run_instance_deploy(docker_image, farm, port, data_dir, log_dir, backup_dir, key, **options):
    logger = logging.getLogger(__name__)
    try:
        maxConns = options.pop('maxConns')
        cacheSize = options.pop('cacheSize')
    except KeyError:
        logger.error('maxConns and cacheSize must be set')
        return -1
    command = [
        '--key', key,
        '--set', 'maxConns={}'.format(maxConns),
        '--set', 'cacheSize={}'.format(cacheSize),
        '--set', 'port={}'.format(port),
        '--set', 'replSet={}'.format(farm),
    ]

    binds = _build_binds(data_dir, log_dir, backup_dir)

    client = get_client(base_url='unix://var/run/docker.sock')
    name = 'mongodb-{}'.format(farm)

    for option, value in options.items():
        command.extend(['--set', '{}={}'.format(option, value)])

    container = client.create_container(
        detach=True,
        name=name,
        image=docker_image,
        command=command,
    )
    container_id = container.get('Id')
    if not container_id:
        raise Exception('failed to create container %s: %s' % (name, container.get('Warnings', '')))

    client.start(container=container_id, network_mode='host', binds=binds)
    if not is_container_running(name):
        raise Exception('failed to start container %s(%s)' % (name, container_id))
    wait_seconds = 600
    while wait_seconds > 0:
        try:
            host_port = '{0}:{1}'.format(socket.gethostname(), port)
            pymongo.MongoClient(host_port)
            break
        except pymongo.errors.ConnectionFailure:
            logger.info("MongoDB instance is not ready, check it later")
            wait_seconds -= 5
            time.sleep(5)
    else:
        raise Exception("Wait so long, MongoDB instance is still not ready")

