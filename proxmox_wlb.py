#!/usr/bin/env python
import os
import logging
import fcntl
import sys
import argparse
import proxmoxer

try:
    import configparser # py3
except ImportError:
    import ConfigParser as configparser # py2

PID_FILE_LOC = '/var/run/proxmox_wlb.pid'
PID_FILE = open(PID_FILE_LOC, 'w')
try:
    fcntl.lockf(PID_FILE, fcntl.LOCK_EX | fcntl.LOCK_NB)
except IOError:
    # another instance is running
    print("Another instance is running, exiting...")
    sys.exit(0)

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
    unbalanced = {'average': 0, 
                  'under': list(), 
                  'over': list(), 
                  'maint': list(),
                  'all': list()}
    maintenance_mode = get_maintenance()
    n = len(set([x[0] for x in xs]))
    max_mem_wanted = sum([x[4] for x in xs]) // n
    max_cpu_wanted = sum([x[2] for x in xs]) // n
    logger.debug("Max size wanted per host: %s GB %s Mhz", max_mem_wanted//1024**3, max_cpu_wanted)
    hosts = set([x[0] for x in xs])
    loads = {}
    average = 0
    for host in hosts:
        load = subsum([x for x in xs if x[0] == host])
        loads[host] = int(subsum([x for x in xs if x[0] == host]))
        average += load
    unbalanced['average'] = average//n
    for host in loads.keys():
        if host in maintenance_mode:
            logger.info("Host %s is in Maintenance mode, trying to clear it off" % host)
            unbalanced['maint'].append([host, loads[host]])
        elif round((float(loads[host])/unbalanced['average']), 2) > 1.2:
            logger.info("Host %s is overloaded: %s", host, loads[host])
            unbalanced['over'].append([host, loads[host]])
            unbalanced['all'].append([host, loads[host]])
        elif round((float(loads[host])/unbalanced['average']), 2) < 0.80:
            logger.info("Host %s is underloaded: %s", host, loads[host])
            unbalanced['under'].append([host, loads[host]])
            unbalanced['all'].append([host, loads[host]])
        else:
            logger.info("Host %s just is: %s", host, loads[host])
            unbalanced['all'].append([host, loads[host]])
    return unbalanced

def migration_planner(unbalanced, nodes_info):
    '''We want to balance the cluster with the minimum number of migrations
       To that end, we don't really mind if hosts are underloaded, so long as
       they aren't overloaded.'''
    moves = []
    for host in unbalanced['maint']:
        hostname = host[0]
        logger.debug("Host %s is in maintenance mode, moving all resources off of it" % hostname)
        # Get a list of all VMs on host
        nodes_filtered = [x for x in nodes_info if x[0] == host[0] and int(x[1]) > 0 ] 
        vm_to_move = min(nodes_filtered, key=lambda x: abs(x[2]*100000+x[4]))
        try:
            # Try to get the lowest loaded server that needs more load
            target_host = min(unbalanced['under'], key=lambda x: x[1])
            logger.info("Selected %s as target host (lowest load)", target_host[0])
        except ValueError:
            # If we can't do that, try for the least loaded server that isn't the current server
            target_host = min([ x for x in unbalanced['all'] if x[0] != host[0]], key=lambda x: x[1])
            logger.warning("Selected %s as target host, but it's not my first choice (no underloaded nodes)", target_host[0])
        logger.warning("Adding following planned move: VM %s from %s to %s",
                       vm_to_move[1], hostname, target_host[0])
        moves.append((hostname, vm_to_move[1], target_host[0]))

    for host in unbalanced['over']:
        hostname = host[0]
        load = host[1]
        average = unbalanced['average']
        logger.debug("Checking host %s with load of %s against average of %s",
                     hostname, load, average)
        to_move_units = load-average
        logger.info("Want to move %s units off of %s", load-average, host[0])
        nodes_filtered = [x for x in nodes_info if x[0] == host[0] and x[6] != True]
        vm_to_move = min(nodes_filtered, key=lambda x: abs(x[2]*100000+x[4]-to_move_units))
        try:
            target_host = min(unbalanced['under'], key=lambda x: x[1])
            logger.info("Selected %s as target host (lowest load)", target_host[0])
        except ValueError:
            target_host = min([ x for x in unbalanced['all'] if x[0] != host[0]], key=lambda x: x[1])
            logger.warning("Selected %s as target host, but it's not my first choice (no underloaded nodes)", target_host[0])
        logger.warning("Adding following planned move: VM %s from %s to %s",
                       vm_to_move[1], hostname, target_host[0])
        moves.append((hostname, vm_to_move[1], target_host[0]))
    return moves

def get_hosts_info(proxmox):
    proxmox_avg_cpu = 2270
    nodes_info = []
    ha_vms = []
    logger.debug("Finding VMs set up for HA")
    for ha_vm in proxmox.cluster.ha.resources.get():
        logger.debug("Found HA VM: %s", ha_vm['sid'])
        ha_vms.append(str(ha_vm['sid']).split(':')[1])
    for node in proxmox.nodes.get():
        if node['status'] == 'offline':
            continue
        # We add an empty set here JUST so that every node shows up
        nodes_info.append([node['node'], 0, 0, 0, 0, 0, True])
        node_max_mhz = node['maxcpu']*proxmox_avg_cpu
        for vm in proxmox.nodes(node['node']).qemu.get():
            logger.debug("Found VM: %s on node %s", vm['vmid'], node['node'])
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
                logger.debug("Got ZeroDivisionError on %s, it's probably turned off", vm['vmid'])
                mhz_usage = 0
            # Gets CPU usage as % of Host
            mhz_pct_host = 100*(mhz_usage/float(node_max_mhz))
            if vm['vmid'] in ha_vms:
                ha = True
            else:
                ha = False
            logger.debug("Finished processing VM: %s ", (vm['vmid']))
            nodes_info.append([node['node'], vm['vmid'], mhz_usage,
                               mhz_pct_host, vm['mem'], mem_pct_host, ha])
    return nodes_info

def get_maintenance():
    maintenance_mode = []
    for file in os.listdir(os.path.dirname(os.path.realpath(__file__))):
        if file.endswith(".maintenance"):
            maintenance_mode.append(file.replace(".maintenance", ""))
    return maintenance_mode

def main():
    # Configure Logging
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.CRITICAL)
    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(ch)

    config = configparser.RawConfigParser()
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="config_file",
                        help="Where the config file is located")
    args = parser.parse_args()
    config.read(args.config_file)
    if config.get('main', 'logdir'):
        fh = logging.FileHandler(config.get('main', 'logdir')+'/proxmox_wlb.log')
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    proxmox = proxmoxer.ProxmoxAPI(config.get('proxmox', 'host'),
                                   user=config.get('proxmox', 'user'),
                                   password=config.get('proxmox', 'password'),
                                   verify_ssl=config.getboolean('proxmox', 'verify_ssl'))

    get_maintenance()
    nodes_info = get_hosts_info(proxmox)
    unbalanced = check_balance(nodes_info)
    if len(unbalanced['over']) > 0:
        moves = migration_planner(unbalanced, nodes_info)
        for move in moves:
            logger.warning("Migrating VM %s from %s to %s", move[1], move[0], move[2])
            try:
                proxmox.nodes(move[0]).qemu(move[1]).migrate.post(target=move[2], online=1)
            except proxmoxer.core.ResourceException as e:
                logger.error("Migration failed with an error: %s | Is another migration already in progress?", e)
    else:
        logger.debug("All hosts appear reasonably balanced")

if __name__ == "__main__":
    logger = logging.getLogger('proxmox_wlb')
    main()
