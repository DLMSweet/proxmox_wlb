#!/usr/bin/env python
import logging
import contextlib
from proxmoxer import ProxmoxAPI
try:
    from http.client import HTTPConnection # py3
except ImportError:
    from httplib import HTTPConnection # py2
from proxmoxer import ProxmoxAPI

try:
    import configparser # py3
except ImportError:
    import ConfigParser as configparser # py2

# Literally just ripped from stackoverflow.
import fcntl, sys
pid_file = '/var/run/proxmox_wlb.pid'
fp = open(pid_file, 'w')
try:
    fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
except IOError:
    # another instance is running
    print("Another instance is running, exiting...")
    sys.exit(0)


###################################################
# Set up some basic logging
###################################################
logger = logging.getLogger('proxmox_wlb')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.WARN)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(ch)
##################################################


def subsum(ys, cpuindex=2, memindex=4):
    # Number chosen because... I don't remember. It works.
    return sum([x[cpuindex]*100000+x[memindex] for x in ys])

def chunk(xs):
    n = len(set([x[0] for x in xs]))
    chunks = []
    xs.sort(key=lambda x: (x[4], x[2]))
    for i in xrange(n):
        chunks.append([])
    while len(xs) > 0:
        sum_map = list(map(subsum, chunks))
        to_add = xs.pop()
        chunks[sum_map.index(min(sum_map))].append(to_add)
    return chunks

def check_balance(xs):
    unbalanced = {'average': 0, 'under': list(), 'over': list()}
    n = len(set([x[0] for x in xs]))
    max_mem_wanted = sum([x[4] for x in xs]) // n
    max_cpu_wanted = sum([x[2] for x in xs]) // n
    logger.info("Max size wanted per host: %s GB %s Mhz" % (max_mem_wanted//1024**3, max_cpu_wanted))
    hosts = set([x[0] for x in xs])
    loads = {}
    average = 0
    for host in hosts:
        load = subsum([x for x in xs if x[0] == host])
        loads[host] = int(subsum([x for x in xs if x[0] == host]))
        average += load
    unbalanced['average'] = average//n
    for host in loads.keys():
        if round((float(loads[host])/unbalanced['average']),2) > 1.5:
            logger.info("Host %s is overloaded: %s" % (host, loads[host]))
            unbalanced['over'].append([host,loads[host]-unbalanced['average']])
        elif round((float(loads[host])/unbalanced['average']),2) < 0.75:
            logger.info("Host %s is underloaded: %s" % (host, loads[host]))
            unbalanced['under'].append([host,loads[host]-unbalanced['average']])
    return unbalanced

def migration_planner(unbalanced, nodes_info):
    '''We want to balance the cluster with the minimum number of migrations
       To that end, we don't really mind if hosts are underloaded, so long as
       they aren't overloaded.'''
    moves = []
    for host in unbalanced['over']:
        hostname = host[0]
        load     = host[1]
        average  = unbalanced['average']
        to_move_units = load-average
        logger.info("Want to move %s units off of %s" % ((load-average), host[0]))
        vm_to_move = min(nodes_info, key=lambda x:abs(x[2]*100000+x[4]-to_move_units))
        target_host = min(unbalanced['under'], key=lambda x:x[1] )
        logger.warn("Adding following planned move: VM %s from %s to %s" % (hostname, vm_to_move[1], target_host[0]))
        moves.append((hostname, vm_to_move[1], target_host[0]))
    return moves
                     
def get_hosts_info(proxmox_host, proxmox_user, proxmox_password, verify_ssl=True):
    proxmox = ProxmoxAPI(proxmox_host, user=proxmox_user,
                         password=proxmox_password, verify_ssl=verify_ssl) 
    proxmox_avg_cpu=2270
    nodes_info = []
    for node in proxmox.nodes.get():
        node_max_mhz = node['maxcpu']*proxmox_avg_cpu
        for vm in proxmox.nodes(node['node']).qemu.get():
            logger.debug("Found VM: %s on node %s" % (vm['vmid'],node['node']))
            # Gets Memory usage as % of Host
            mem_pct_host = 100*(int(vm['maxmem'])/float(node['maxmem']))
            # Gets CPU usage in Mhz based on an average of the last hour:
            try:
                cpu_usage = [] 
                for x in proxmox.nodes(node['node']).qemu(vm['vmid']).rrddata.get(timeframe='hour'):
                    try:
                        cpu_usage.append(x['cpu'])
                    except KeyError:
                        pass
                cpu_usage = sum(cpu_usage)/float(len(cpu_usage))
                mhz_usage = int((proxmox_avg_cpu*vm['cpus'])*cpu_usage)
            except ZeroDivisionError:
                logger.debug("Got ZeroDivisionError on %s, it's probably turned off" % vm['vmid'])
                mhz_usage = 0
            # Gets CPU usage as % of Host
            mhz_pct_host = 100*(mhz_usage/float(node_max_mhz))
            logger.debug("Finished processing VM: %s " % (vm['vmid']))
            nodes_info.append([node['node'],vm['vmid'],mhz_usage,mhz_pct_host,vm['mem'], mem_pct_host])
    return nodes_info
        
if __name__ == "__main__":
    config = configparser.RawConfigParser()
    if sys.argv[1]:
        config.read(sys.argv[1])
    else:
        config.read('proxmox_wlb.cfg')
    if config.get('main', 'logdir'):
        fh = logging.FileHandler(config.get('main', 'logdir')+'/proxmox_wlb.log')
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
    nodes_info = get_hosts_info(config.get('proxmox', 'host'), 
                                config.get('proxmox', 'user'),
                                config.get('proxmox', 'password'),
                                config.getboolean('proxmox', 'verify_ssl'))
    unbalanced = check_balance(nodes_info)
    if len(unbalanced['over'])>0:
        moves = migration_planner(unbalanced, nodes_info)
        for move in moves:
            logger.warn("Would migrate VM %s from %s to %s" % (move[1], move[0], move[2]))
    else:
        logger.info("All hosts appear reasonably balanced") 


