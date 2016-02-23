YARN EC2
========
This is a script to help you quickly setup a YARN cluster on EC2.

Features
--------
- Dynamically add or remove slave nodes to the cluster.
- Customized installation of packages.

How to Use
----------
To start a cluster, the script follows two steps: (1) start master machine; (2) add slaves.
This two step procedure allows you to add and remove slaves on the fly.

- Start your master machine
```bash
./yarn_ec2.py -k mykey -i mypem.pem launch cluster-name
```
- Add slaves to the cluster
```bash
./yarn_ec2.py -k mykey -i mypem.pem -s nslave addslave cluster-name
```
- Alternatively, you can add spot instance to the cluster
```bash
./yarn_ec2.py -k mykey -i mypem.pem -s nslave addspot cluster-name
```
- Both addslave and addspot will send request to EC2 and may not be fullfilled immediately
  - They will connect to the master node after one bootstrap (which takes around 1 minimute).
  - You can browse the yarn resource manager for the status of the cluster.
- Shutdown the machines manually in ec2 panel

Distributed Storage
-------------------
Because the cluster is dynamic, all the nodes are only used as computing nodes.
HDFS is only started on the master machine for temp code transfer.
Normally S3 is used instead for distributed storage.


Customize Installation
----------------------
You can modify ```custom_master_install```` and ```custom_all_nodes_install``` in bootstrap.py to
add installation script of the packages you wanted.


Restart Master Machine
----------------------
In case you stopped the master and restart it on the EC2. There is no need to do the launch step again.
Instead, log into the master machine, and run ```startup.sh``` on the home folder.
After the startup is finished, you can continue with the steps of adding slaves.
