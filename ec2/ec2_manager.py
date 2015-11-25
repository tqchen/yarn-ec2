#!/usr/bin/env python
import ec2_util
import mode_ec2
import logging
import time
import datetime
from boto import ec2

class EC2Error(Exception):
    pass

class EC2Manager(object):
    """
    Maneger class to maintain cluster
    """
    def __init__(self, opts, poll_gap_sec = 10):
        self.opts = opts
        self.poll_gap_sec = poll_gap_sec
        self.conn = ec2.connect_to_region(opts.region)
        cluster_name = opts.cluster_name
        self.slave_group = ec2_util.get_or_make_group(self.conn,
                                                      cluster_name + "-slave",
                                                      False)
        # Check if instances are already running in our groups
        existing_masters, existing_slaves = ec2_util.get_existing_cluster(self.conn, cluster_name,
                                                                          die_on_error=False)
        if len(existing_masters) == 0:
            raise EC2Error("ERROR: Cannot find master machine on group" +
                           "group %s" % (master_group.name))
        self.master =  existing_masters[0]
        vcpu, vram, price = mode_ec2.get_resource_map()
        self.rmap_price = price
        self.rmap_vcpu = vcpu
        self.rmap_vram = vram
        self.spot_req_map = {}
        self.price_hist = {}
        self.on_demand_nodes = []
        logging.info('start maintaining cluster, master=%s' % self.master.private_dns_name)

    def on_price_change(self, chg_list):
        pass
    
    def on_spot_state_update(self, new_state, old_state):        
        pass
    
    def request_spot(self, instance_type, price, nworker = 1):
        zone = self.master.placement
        ami = mode_ec2.get_ami(instance_type)
        block_map = ec2_util.get_block_device(instance_type, 0)
        user_data = mode_ec2.get_user_data('bootstrap.py',
                                           self.master.private_dns_name,
                                           instance_type)
        slave_reqs = self.conn.request_spot_instances(
            price = price,
            image_id = ami,
            placement = zone,
            count = nworker,
            key_name = self.opts.key_pair,
            security_groups = [self.slave_group],
            instance_type = instance_type,
            block_device_map = block_map,
            user_data = user_data)
        logging.info('Request spot instance type=%s nworker=%d max-price=%s'\
                         % (instance_type, nworker, str(price)))
        for r in slave_reqs:
            assert r.id not in self.spot_req_map
            self.spot_req_map[r.id] = r
        
    def request_slave(self, instance_type, nworker = 1):
        zone = self.master.placement
        ami = mode_ec2.get_ami(instance_type)
        block_map = ec2_util.get_block_device(instance_type, 0)
        image = self.conn.get_all_images(image_ids = [ami])[0]
        user_data = mode_ec2.get_user_data('bootstrap.py',
                                           self.master.private_dns_name,
                                           instance_type)        
        slave_res = image.run(key_name = self.opts.key_pair,
                              security_groups = [self.slave_group],
                              instance_type = instance_type,
                              placement = zone,
                              min_count = nworker,
                              max_count = nworker,
                              block_device_map = block_map,
                              user_data = user_data)
        price = self.rmap_price[instance_type]
        logging.info('Request on-demand instance type=%s nworker=%d normal-price=%s'\
                         % (instance_type, nworker, str(price)))
        self.on_demand_nodes += slave_res

    def refresh_monitor(self):
        # update spot instance request
        reqs = self.conn.get_all_spot_instance_requests()
        for r in reqs:
            if r.id not in self.spot_req_map:
                continue
            req = self.spot_req_map[r.id]

            if r.state != req.state:
                logging.info('inst %s state change from %s to %s' % (r.id, req.state, r.state))
                self.spot_req_map[r.id] = r
                self.on_spot_state_update(r, req.state)
                if r.state == 'closed' or r.state == 'canceled' or r.state == 'failed':
                    self.spot_req_map.pop(r.id, None)

    def refresh_price(self):
        # update price monitor of the spots
        end = datetime.datetime.now()
        begin = end - datetime.timedelta(minutes = 10)
        hist = self.conn.get_spot_price_history(start_time = begin.isoformat(),
                                                end_time = end.isoformat(),
                                           product_description = 'Linux/UNIX')
        hist.sort(key = lambda x : x.timestamp)
        pchg = []
        for x in hist:
            key = (x.instance_type, x.availability_zone)
            val = (x.price, x.timestamp)
            if key not in self.price_hist:
                self.price_hist[key] = [val]
                pchg.append(key)
            else:
                vec = self.price_hist[key]
                if x.timestamp <= vec[-1][1]:
                    continue
                if len(vec) != 1 and vec[-1][0] == val[0]:
                    vec.pop(-1)
                else:
                    pchg.append(key)
                vec.append(val)

        if len(pchg) != 0:
            self.on_price_change(pchg)

        return pchg

    def refresh(self):
        self.refresh_price()
        self.refresh_monitor()
    
    def run(self):
        while True:
            self.refresh()
            time.sleep(self.poll_gap_sec)

class SimpleManager(EC2Manager):
    def __init__(self, opts):
        super(self.__class__, self).__init__(opts)
        self.nworker = opts.slaves
        self.inst_type = opts.instance_type
        self.refresh()
        self.init_req(self.nworker,
                      self.opts.min_ratio,
                      self.opts.max_ratio)

    def on_spot_state_update(self, old_state, new_state):
        nactive = len([s for s in self.spot_req_map if s.state == 'active'])
        ntotal = len(self.spot_req_map)
        if ntotal + len(self.on_demand_nodes) < self.nworker:
            self.init_req(self.nworker - ntotal - len(self.on_demand_nodes),
                          self.opts.min_ratio, self.opts.max_ratio)
        
    def on_price_change(self, chg_list):
        pass
    
    def init_req(self, n, min_ratio, max_ratio, req_gap = 20):
        inst_type = self.inst_type
        zone = self.master.placement
        key = (inst_type, zone)
        price = self.rmap_price[inst_type]
        sprice = self.price_hist[key][-1][0]
        min_price = price * min_ratio
        max_price = price * max_ratio
        logging.info('Trying to get %d %s machines spot-price=%g, range=[%g, %g]'\
                     % (n, inst_type, sprice, min_price, max_price))
        if sprice > min_price:
            min_price = sprice * 1.05
        if min_price * 1.1 > max_price:
            self.request_slave(inst_type, n)
        else:
            step = (max_price - min_price) / n
            for i in range(1, n + 1):
                self.request_spot(inst_type, min_price + i * step)
                time.sleep(req_gap)
        
def config_logger(args):
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    level = eval('logging.' + args.log_level)
    if args.log_file is None:
        logging.basicConfig(format=FORMAT, level = level)
    else:
        logging.basicConfig(format=FORMAT, level = level, filename = args.log_file)
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter(FORMAT))
        console.setLevel(level)
        logging.getLogger('').addHandler(console)
