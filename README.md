YARN EC2
========
This is a script to help you quickly setup a dynamic YARN cluster on EC2.
It adapts a cloud workflow to use S3 for distributed data storage and YARN for computing.

***Features***
- Dynamically add or remove slave nodes from the cluster.
- Customized installation of packages.

How to Use
----------
To start a cluster, the script follows two steps: (1) start master machine; (2) add slaves.
This two step procedure allows you to add and remove slaves on the fly.

- Start your master machine
  - ```./yarn-ec2 -k mykey -i mypem.pem launch cluster-name ```
- Add slaves to the cluster
  - ```./yarn-ec2 -k mykey -i mypem.pem -s nslave addslave cluster-name ```
- Alternatively, you can add spot instance to the cluster
  - ```./yarn-ec2 -k mykey -i mypem.pem -s nslave addspot cluster-name```
  - On demand price is used by default, you can change it by ```--spot-price``` option.
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
You can modify ```custom_master_install``` and ```custom_all_nodes_install``` in [bootstrap.py](https://github.com/tqchen/yarn-ec2/blob/master/bootstrap.py#L21)
to add the packages you like to install on each machine.


Restart Master Machine
----------------------
In case you stopped the master and restart it on the EC2. There is no need to do the launch step again.
Instead, log into the master machine, and run ```startup.sh``` on the home folder.
After the startup is finished, you can continue with the steps of adding slaves.




Acknowledgement
---------------
Part of yarn-ec2 is adopted from [spark-ec2](https://github.com/amplab/spark-ec2) script.
