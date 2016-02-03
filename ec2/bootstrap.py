#!/usr/bin/env python
# encoding: utf-8
"""
script to install all the necessary things
for working on a linux machine with nothing

Installing minimum dependencies
"""
import sys
import os
import logging
import subprocess
import xml.etree.ElementTree as ElementTree
import xml.dom.minidom as minidom
import socket
import time
import pwd

###---------------------------------------------------##
#  Configuration Section, will be modified by script  #
###---------------------------------------------------##
node_apt_packages = [
    'emacs',
    'git',
    'g++',
    'make',
    'python-numpy',
    'libprotobuf-dev',
    'libcurl4-openssl-dev']

# master only packages
master_apt_packages = [
    'protobuf-compiler']

# List of r packages to be installed in master
master_r_packages = [
    'r-base-dev',
    'r-base',
    'r-cran-statmod',
    'r-cran-RCurl',
    'r-cran-rjson'
]

# download link of hadoop.
hadoop_url = 'http://www.motorlogy.com/apache/hadoop/common/hadoop-2.6.0/hadoop-2.6.0.tar.gz'
hadoop_dir = 'hadoop-2.6.0'

# customized installation script.
# See optional installation scripts for options.
def custom_master_install():
    install_spark()
    install_r()
    pass

# customized installation script for all nodes.
def custom_all_nodes_install():
    install_gcc()
    pass

###---------------------------------------------------##
#  Automatically set by script                        #
###---------------------------------------------------##
USER_NAME = 'ubuntu'
# setup variables
MASTER = os.getenv('MY_MASTER_DNS', '')
# node type the type of current node
NODE_TYPE = os.getenv('MY_NODE_TYPE', 'm3.xlarge')
NODE_VMEM = int(os.getenv('MY_NODE_VMEM', str(1024*15)))
NODE_VCPU = int(os.getenv('MY_NODE_VCPU', '4'))
AWS_ID = os.getenv('AWS_ACCESS_KEY_ID', 'undefined')
AWS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'undefined')
JAVA_HOME = os.getenv('JAVA_HOME')
HADOOP_HOME = os.getenv('HADOOP_HOME')
DISK_LIST = [('xvd' + chr(ord('b') + i)) for i in range(10)]
ENVIRON = os.environ.copy()

###--------------------------------##
#  Optional installation scripts.  #
###--------------------------------##
def install_r():
    if master_r_packages:
        sudo("apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E084DAB9")
        sudo("echo deb https://cran.r-project.org/bin/linux/ubuntu trusty/ >>/etc/apt/sources.list")
        sudo('apt-get -y update')
        sudo('apt-get -y install %s' % (' '.join(master_r_packages)))


def install_spark():
    run('wget http://apache.osuosl.org/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.4.tgz')
    run('tar xf spark-1.6.0-bin-hadoop2.4.tgz')
    run('rm -rf spark-1.6.0-bin-hadoop2.4.tgz')
    with open('.bashrc', 'a') as fo:
        fo.write('\nexport PATH=${PATH}:spark-1.6.0-bin-hadoop2.4\n')


def install_xgboost():
    run('git clone --recursive https://github.com/dmlc/xgboost')
    run('cd xgboost; cp make/config.mk .; echo USE_S3=1 >> config.mk; make -j4')

### Script section ###
def run(cmd):
    try:
        print cmd
        logging.info(cmd)
        proc = subprocess.Popen(cmd, shell=True, env = ENVIRON,
                                stdout=subprocess.PIPE, stderr = subprocess.PIPE)
        out, err = proc.communicate()
        retcode = proc.poll()
        if retcode != 0:
            logging.error('Command %s returns %d' % (cmd,retcode))
            logging.error(out)
            logging.error(err)
        else:
            print out
    except Exception as e:
        print(str(e))
        logging.error('Exception running: %s' % cmd)
        logging.error(str(e))
        pass

def sudo(cmd):
    run('sudo %s' % cmd)

### Installation helpers ###
def install_packages(pkgs):
    sudo('apt-get -y update')
    sudo('apt-get -y install %s' % (' '.join(pkgs)))

# install g++4.9, needed for regex match.
def install_gcc():
    sudo('add-apt-repository -y ppa:ubuntu-toolchain-r/test')
    sudo('apt-get -y update')
    sudo('apt-get -y install g++-4.9')

def install_java():
    """
    install java and setup environment variables
    Returns environment variables that needs to be exported
    """
    if not os.path.exists('jdk1.8.0_40'):
        run('wget --no-check-certificate --no-cookies'\
                ' --header \"Cookie: oraclelicense=accept-securebackup-cookie\"'\
                ' http://download.oracle.com/otn-pub/java/jdk/8u40-b26/jdk-8u40-linux-x64.tar.gz')
        run('tar xf jdk-8u40-linux-x64.tar.gz')
        run('rm -f jdk-8u40-linux-x64.tar.gz')
    global JAVA_HOME
    if JAVA_HOME is None:
        JAVA_HOME = os.path.abspath('jdk1.8.0_40')
    return [('JAVA_HOME', JAVA_HOME)]


def install_hadoop(is_master):
    def update_site(fname, rmap):
        """
        update the site script
        """
        try:
            tree = ElementTree.parse(fname)
            root = tree.getroot()
        except Exception:
            cfg = ElementTree.Element("configuration")
            tree = ElementTree.ElementTree(cfg)
            root = tree.getroot()
        rset = set()
        for prop in root.getiterator('property'):
            prop = dict((p.tag, p) for p in prop)
            name = prop['name'].text.strip()
            if name in rmap:
                prop['value'].text = str(rmap[name])
                rset.add(name)
        for name, text in rmap.iteritems():
            if name in rset:
                continue
            prop = ElementTree.SubElement(root, 'property')
            ElementTree.SubElement(prop, 'name').text = name
            ElementTree.SubElement(prop, 'value').text = str(text)
        rough_string = ElementTree.tostring(root, 'utf-8')
        reparsed = minidom.parseString(rough_string)
        pretty = reparsed.toprettyxml(indent='\t')
        fo = open(fname, 'w')
        fo.write(pretty)
        fo.close()

    def setup_hadoop_site(master, hadoop_dir, hdfs_dir, vcpu, vmem):
        """
        setup hadoop side given the parameters

        Parameters
        ----------
        master: the dns to master uri
        hadoop_dir: the directory to store temp files
        hdfs_dir: the directories for hdfs
        vcpu: the number of cpus current machine have
        vmem: the memory(MB) current machine have
        """
        if vmem < 4 * 1024:
            reserved_ram = 256
        elif vmem < 8 * 1024:
            reserved_ram = 1 * 1024
        elif vmem < 24 * 1024 :
            reserved_ram = 2 * 1024
        elif vmem < 48 * 1024:
            reserved_ram = 2 * 1024
        elif vmem < 64 * 1024:
            reserved_ram = 6 * 1024
        else:
            reserved_ram = 8 * 1024
        ram_per_container = (vmem - reserved_ram) / vcpu

        if is_master:
            vcpu = vcpu - 2

        tmp_dir = hadoop_dir[0]
        core_site = {
            'fs.defaultFS': 'hdfs://%s:9000/' % master,
            'fs.s3n.impl': 'org.apache.hadoop.fs.s3native.NativeS3FileSystem',
            'hadoop.tmp.dir': tmp_dir
        }
        if AWS_ID != 'undefined':
            core_site['fs.s3n.awsAccessKeyId'] = AWS_ID
            core_site['fs.s3n.awsSecretAccessKey'] = AWS_KEY

        update_site('%s/etc/hadoop/core-site.xml' % HADOOP_HOME, core_site)
        hdfs_site = {
            'dfs.data.dir': ','.join(['%s/data' % d for d in hdfs_dir]),
            'dfs.permissions': 'false',
            'dfs.replication': '1'
        }
        update_site('%s/etc/hadoop/hdfs-site.xml' % HADOOP_HOME, hdfs_site)
        yarn_site = {
            'yarn.resourcemanager.resource-tracker.address': '%s:8025' % master,
            'yarn.resourcemanager.scheduler.address': '%s:8030' % master,
            'yarn.resourcemanager.address': '%s:8032' % master,
            'yarn.scheduler.minimum-allocation-mb': 512,
            'yarn.scheduler.maximum-allocation-mb': 640000,
            'yarn.scheduler.minimum-allocation-vcores': 1,
            'yarn.scheduler.maximum-allocation-vcores': 32,
            'yarn.nodemanager.resource.memory-mb': vcpu * ram_per_container,
            'yarn.nodemanager.resource.cpu-vcores': vcpu,
            'yarn.log-aggregation-enable': 'true',
            'yarn.nodemanager.vmem-check-enabled': 'false',
            'yarn.nodemanager.aux-services': 'mapreduce_shuffle',
            'yarn.nodemanager.aux-services.mapreduce.shuffle.class': 'org.apache.hadoop.mapred.ShuffleHandler',
            'yarn.nodemanager.remote-app-log-dir': os.path.join(tmp_dir, 'logs'),
            'yarn.nodemanager.log-dirs': os.path.join(tmp_dir, 'userlogs'),
            'yarn.nodemanager.local-dirs': ','.join(['%s/yarn/nm-local-dir' % d for d in hadoop_dir])
        }
        update_site('%s/etc/hadoop/yarn-site.xml' % HADOOP_HOME, yarn_site)
        mapred_site = {
            'mapreduce.application.classpath' : ':'.join(['$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*',
                                                          '$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*',
                                                          '$HADOOP_MAPRED_HOME/share/hadoop/tools/lib/*']),
            'yarn.app.mapreduce.am.resource.mb': 2 * ram_per_container,
            'yarn.app.mapreduce.am.command-opts': '-Xmx%dm' % int(0.8 * 2 * ram_per_container),
            'mapreduce.framework.name': 'yarn',
            'mapreduce.map.cpu.vcores': 1,
            'mapreduce.map.memory.mb': ram_per_container,
            'mapreduce.map.java.opts': '-Xmx%dm' % int(0.8 * ram_per_container),
            'mapreduce.reduce.cpu.vcores': 1,
            'mapreduce.reduce.memory.mb': 2 * ram_per_container,
            'mapreduce.reduce.java.opts': '-Xmx%dm' % int(0.8 * ram_per_container)
        }
        update_site('%s/etc/hadoop/mapred-site.xml' % HADOOP_HOME, mapred_site)
        capacity_site = {
            'yarn.scheduler.capacity.resource-calculator': 'org.apache.hadoop.yarn.util.resource.DominantResourceCalculator'
        }
        update_site('%s/etc/hadoop/capacity-scheduler.xml' % HADOOP_HOME, capacity_site)
        fo = open('%s/etc/hadoop/hadoop-env.sh' % HADOOP_HOME, 'w')
        fo.write('export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOP_PREFIX/share/hadoop/tools/lib/*\n')
        fo.write('export HADOOP_LOG_DIR=%s/log\n' % tmp_dir)
        fo.write('export YARN_LOG_DIR=%s/log\n' % tmp_dir)
        fo.write('export JAVA_HOME=\"%s\"\n' % JAVA_HOME)
        fo.close()
        fo = open('%s/etc/hadoop/slaves' % HADOOP_HOME, 'w')
        fo.write(master + '\n')
        fo.close()

    def run_install():
        if not os.path.exists('hadoop-2.6.0'):
            run('wget %s' % hadoop_url)
            run('tar xf hadoop-2.6.0.tar.gz')
            run('rm -f hadoop-2.6.0.tar.gz')
            global HADOOP_HOME
        if HADOOP_HOME is None:
            HADOOP_HOME = os.path.abspath('hadoop-2.6.0')
        env = [('HADOOP_HOME', HADOOP_HOME)]
        env += [('HADOOP_PREFIX', HADOOP_HOME)]
        env += [('HADOOP_MAPRED_HOME', HADOOP_HOME)]
        env += [('HADOOP_COMMON_HOME', HADOOP_HOME)]
        env += [('HADOOP_HDFS_HOME', HADOOP_HOME)]
        env += [('YARN_HOME', HADOOP_HOME)]
        env += [('YARN_CONF_DIR', '%s/etc/hadoop' % HADOOP_HOME)]
        env += [('HADOOP_CONF_DIR', '%s/etc/hadoop' % HADOOP_HOME)]
        disks = ['/disk/%s' % d for d in DISK_LIST if os.path.exists('/dev/%s' % d)]
        setup_hadoop_site(MASTER,
                          ['%s/hadoop' % d for d in disks],
                          ['%s/hadoop/dfs' % d for d in disks],
                          NODE_VCPU, NODE_VMEM)
        return env

    return run_install()

def regsshkey(fname):
    for dns in (open(fname).readlines() + ['localhost', '0.0.0.0']):
        try:
            run('ssh-keygen -R %s' % dns.strip())
        except:
            pass
        run('ssh-keyscan %s >> ~/.ssh/known_hosts' % dns.strip())

# main script to install all dependencies
def install_main(is_master):
    if is_master:
        install_packages(master_apt_packages + node_apt_packages)
    else:
        install_packages(node_apt_packages)

    env = []
    env += install_java()
    env += install_hadoop(is_master)
    path = ['$HADOOP_HOME/bin', '$HADOOP_HOME/sbin', '$JAVA_HOME/bin']
    env += [('LD_LIBRARY_PATH', '$HADOOP_HOME/native/lib')]
    env += [('LD_LIBRARY_PATH', '${LD_LIBRARY_PATH}:$HADOOP_HDFS_HOME/lib/native:$JAVA_HOME/jre/lib/amd64/server')]
    env += [('LD_LIBRARY_PATH', '${LD_LIBRARY_PATH}:/usr/local/lib')]
    env += [('LIBHDFS_OPTS', '--Xmx128m')]
    env += [('MY_MASTER_DNS', MASTER)]
    env += [('MY_NODE_TYPE', NODE_TYPE)]
    env += [('MY_NODE_VMEM', str(NODE_VMEM))]
    env += [('MY_NODE_VCPU', str(NODE_VCPU))]
    if AWS_ID != 'undefined':
        env += [('AWS_ACCESS_KEY_ID', AWS_ID)]
    if AWS_KEY != 'undefined':
        env += [('AWS_SECRET_ACCESS_KEY', AWS_KEY)]
    # setup environments
    fo = open('.hadoop_env', 'w')
    for k, v in env:
        fo.write('export %s=%s\n' % (k,v))
        ENVIRON[k] = v
    fo.write('export PATH=$PATH:%s\n' % (':'.join(path)))
    fo.write('export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib\n')
    fo.close()
    for l in open('.bashrc'):
        if l.find('.hadoop_env') != -1:
            return
    run('echo source ~/.hadoop_env >> ~/.bashrc')
    # allow ssh, if they already share the key.
    key_setup = """
        [ -f ~/.ssh/id_rsa ] ||
            (ssh-keygen -q -t rsa -N '' -f ~/.ssh/id_rsa &&
             cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys)
    """
    run(key_setup)
    regsshkey('%s/etc/hadoop/slaves' % HADOOP_HOME)
    # end of instalation.

# Make startup script for bulding
def make_startup_script(is_master):
    assert JAVA_HOME is not None
    assert HADOOP_HOME is not None
    assert NODE_VCPU is not None
    assert NODE_VMEM is not None
    disks = []
    cmds = []

    if is_master:
        cmds.append('$HADOOP_HOME/sbin/stop-all.sh')

    for d in DISK_LIST:
        if os.path.exists('/dev/%s' % d):
            cmds.append('sudo umount /dev/%s' % d)
            cmds.append('sudo mkfs -t ext4 /dev/%s' % d)
            cmds.append('sudo mkdir -p /disk/%s' % d)
            cmds.append('sudo mount /dev/%s /disk/%s' % (d, d))
            disks.append('/disk/%s' % d)

    for d in disks:
        cmds.append('sudo mkdir -p %s/hadoop' %d)
        cmds.append('sudo chown ubuntu:ubuntu %s/hadoop' % d)
        cmds.append('sudo mkdir -p %s/tmp' %d)
        cmds.append('sudo chown ubuntu:ubuntu %s/tmp' % d)
        cmds.append('rm -rf %s/hadoop/dfs' % d)
        cmds.append('mkdir %s/hadoop/dfs' % d)
        cmds.append('mkdir %s/hadoop/dfs/name' % d)
        cmds.append('mkdir %s/hadoop/dfs/data' % d)

    # run command
    if is_master:
        cmds.append('$HADOOP_HOME/bin/hadoop namenode -format')
        cmds.append('$HADOOP_HOME/sbin/start-all.sh')
    else:
        cmds.append('export HADOOP_LIBEXEC_DIR=$HADOOP_HOME/libexec &&'\
                ' $HADOOP_HOME/sbin/yarn-daemon.sh --config $HADOOP_HOME/etc/hadoop start nodemanager')
    with open('startup.sh', 'w') as fo:
        fo.write('#!/bin/bash\n')
        fo.write('set -v\n')
        fo.write('\n'.join(cmds))
    run('chmod +x startup.sh')
    run('./startup.sh')


def main():
    global MASTER
    logging.basicConfig(filename = 'bootstrap.log', level = logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    if MASTER == '':
        is_master = True
        MASTER = socket.getfqdn()
        logging.info('assuming master is myself as %s' % MASTER)
    else:
        is_master = socket.getfqdn() == MASTER
    tstart = time.time()
    install_main(is_master)
    tmid = time.time()
    logging.info('installation finishes in %g secs' % (tmid - tstart))
    make_startup_script(is_master)
    ENVIRON['HADOOP_HOME'] = HADOOP_HOME
    ENVIRON['JAVA_HOME'] = JAVA_HOME
    tend = time.time()
    if is_master:
        custom_master_install()
    custom_all_nodes_install()
    logging.info('boostrap finishes in %g secs' % (tend - tmid))
    logging.info('all finishes in %g secs' % (tend - tstart))

if __name__ == '__main__':
    pw_record = pwd.getpwnam(USER_NAME)
    user_name = pw_record.pw_name
    user_home_dir = pw_record.pw_dir
    user_uid = pw_record.pw_uid
    user_gid = pw_record.pw_gid
    env = os.environ.copy()
    cwd = user_home_dir
    ENVIRON['HOME'] = user_home_dir
    os.setgid(user_gid)
    os.setuid(user_uid)
    os.chdir(user_home_dir)
    main()
