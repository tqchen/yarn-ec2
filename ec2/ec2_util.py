#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import boto
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType, EBSBlockDeviceType
from boto import ec2
import sys
import string
import time

# Get number of local disks available for a given EC2 instance type.
def get_num_disks(instance):
    # From http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html
    # Updated 2014-6-20
    disks_by_instance = {
        "m1.small":    1,
        "m1.medium":   1,
        "m1.large":    2,
        "m1.xlarge":   4,
        "t1.micro":    1,
        "c1.medium":   1,
        "c1.xlarge":   4,
        "m2.xlarge":   1,
        "m2.2xlarge":  1,
        "m2.4xlarge":  2,
        "cc1.4xlarge": 2,
        "cc2.8xlarge": 4,
        "cg1.4xlarge": 2,
        "hs1.8xlarge": 24,
        "cr1.8xlarge": 2,
        "hi1.4xlarge": 2,
        "m3.medium":   1,
        "m3.large":    1,
        "m3.xlarge":   2,
        "m3.2xlarge":  2,
        "i2.xlarge":   1,
        "i2.2xlarge":  2,
        "i2.4xlarge":  4,
        "i2.8xlarge":  8,
        "c3.large":    2,
        "c3.xlarge":   2,
        "c3.2xlarge":  2,
        "c3.4xlarge":  2,
        "c3.8xlarge":  2,
        "r3.large":    1,
        "r3.xlarge":   1,
        "r3.2xlarge":  1,
        "r3.4xlarge":  1,
        "r3.8xlarge":  2,
        "g2.2xlarge":  1,
        "g2.8xlarge":  2,
        "t1.micro":    0
    }
    if instance in disks_by_instance:
        return disks_by_instance[instance]
    else:
        print >> sys.stderr, ("WARNING: Don't know number of disks on instance type %s; assuming 1"
                              % instance)
        return 1

def get_instance_type(instance):
    instance_types = {
        "m1.small":    "pvm",
        "m1.medium":   "pvm",
        "m1.large":    "pvm",
        "m1.xlarge":   "pvm",
        "t1.micro":    "pvm",
        "c1.medium":   "pvm",
        "c1.xlarge":   "pvm",
        "m2.xlarge":   "pvm",
        "m2.2xlarge":  "pvm",
        "m2.4xlarge":  "pvm",
        "cc1.4xlarge": "hvm",
        "cc2.8xlarge": "hvm",
        "cg1.4xlarge": "hvm",
        "hs1.8xlarge": "pvm",
        "hi1.4xlarge": "pvm",
        "m3.medium":   "hvm",
        "m3.large":    "hvm",
        "m3.xlarge":   "hvm",
        "m3.2xlarge":  "hvm",
        "cr1.8xlarge": "hvm",
        "i2.xlarge":   "hvm",
        "i2.2xlarge":  "hvm",
        "i2.4xlarge":  "hvm",
        "i2.8xlarge":  "hvm",
        "c3.large":    "pvm",
        "c3.xlarge":   "pvm",
        "c3.2xlarge":  "pvm",
        "c3.4xlarge":  "pvm",
        "c3.8xlarge":  "pvm",
        "r3.large":    "hvm",
        "g2.2xlarge":  "hvm",
        "g2.8xlarge":  "hvm",
        "r3.xlarge":   "hvm",
        "r3.2xlarge":  "hvm",
        "r3.4xlarge":  "hvm",
        "r3.8xlarge":  "hvm",
        "t2.micro":    "hvm",
        "t2.small":    "hvm",
        "t2.medium":   "hvm"
    }
    if instance in instance_types:
        return instance_types[instance]
    else:
        print >> sys.stderr,\
            "Don't recognize %s, assuming type is pvm" % instance
        return 'pvm'

# Wait for a set of launched instances to exit the "pending" state
# (i.e. either to start running or to fail and be terminated)
def wait_for_instances(conn, instances):
    while True:
        for i in instances:
            i.update()
        status = conn.get_all_instance_status(instance_ids = [i.id for i in instances])
        if len([i for i in instances if i.state == 'pending']) > 0:
            time.sleep(5)
        elif len([i for i in status if i.system_status.status == 'initializing']) > 0:
            time.sleep(5)
        else:
            return

# Get the EC2 security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name, make_if_not_exist = True):
    groups = conn.get_all_security_groups()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        if not make_if_not_exist:
            print >> sys.stderr, "ERROR: Could not find any existing security group"
            sys.exit(1)
        print "Creating security group " + name
        return conn.create_security_group(name, "MODE EC2 group")

# Check whether a given EC2 instance object is in a state we consider active,
# i.e. not terminating or terminated. We count both stopping and stopped as
# active since we can restart stopped clusters.
def is_active(instance):
    return (instance.state in ['pending', 'running', 'stopping', 'stopped'])

# Attempt to resolve an appropriate AMI given the architecture and
# region of the request.
# Information regarding Amazon Linux AMI instance type was update on 2014-6-20:
# http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/
def get_block_device(instance_type, ebs_vol_size):
    block_map = BlockDeviceMapping()

    if ebs_vol_size > 0:
        device = EBSBlockDeviceType()
        device.size = ebs_vol_size
        device.delete_on_termination = True
        block_map["/dev/sdv"] = device

    if instance_type.startswith('m3.'):
        for i in range(get_num_disks(instance_type)):
            dev = BlockDeviceType()
            dev.ephemeral_name = 'ephemeral%d' % i
            # The first ephemeral drive is /dev/sdb.
            name = '/dev/sd' + string.letters[i + 1]
            block_map[name] = dev
    return block_map


# Get the EC2 instances in an existing cluster if available.
# Returns a tuple of lists of EC2 instance objects for the masters and slaves
def get_existing_cluster(conn, cluster_name, die_on_error=True):
    print "Searching for existing cluster " + cluster_name + "..."
    reservations = conn.get_all_instances()
    master_nodes = []
    slave_nodes = []
    for res in reservations:
        active = [i for i in res.instances if is_active(i)]
        for inst in active:
            group_names = [g.name for g in inst.groups]
            if group_names == [cluster_name + "-master"]:
                master_nodes.append(inst)
            elif group_names == [cluster_name + "-slaves"]:
                slave_nodes.append(inst)
    if any((master_nodes, slave_nodes)):
        print ("Found %d master(s), %d slaves" % (len(master_nodes), len(slave_nodes)))
    if master_nodes != [] or not die_on_error:
        return (master_nodes, slave_nodes)
    else:
        if master_nodes == [] and slave_nodes != []:
            print >> sys.stderr, "ERROR: Could not find master in group " + cluster_name + "-master"
        else:
            print >> sys.stderr, "ERROR: Could not find any existing cluster"
        sys.exit(1)

