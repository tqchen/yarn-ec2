Bootstrap Script to Install YARN on EC2
===
* ```./yarn_ec2.py launch cluster-name```
  - Wait until the master node starts
  - This will start master node, with YARN support
* ```./yarn_ec2.py -s nslave addslave cluster-name``
  - Add slaves to the cluster
  - This will start the slaves and have each slaves report to the master node
  - You can find the slave machines in yarn cluster tracker
  - Note that links in yarn cluster tracker do not function fully because of internal IP address
* Shutdown the machines manually in ec2 panel

Running DMLC Jobs
=================
* Log into master machine
* Checkout wormhole
* Add ```dmlc-core/tracker``` to PATH
* Add enviroment variable ```AWS_ACCESS_KEY_ID``` and ```AWS_SECRET_ACCESS_KEY```

Note
====
Everytime you shutdown the master and restart it again, run ```bootstrap.py```.

