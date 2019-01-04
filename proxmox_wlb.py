#!/usr/bin/env python
import os
import logging
import fcntl
import sys
import argparse
import statistics
import time
import configparser
import proxmoxer


class ProxmoxVM():
    """A single KVM/QEMU VM on Proxmox"""
    # pylint: disable=too-many-instance-attributes
    def __init__(self, name, id, ha=False):
        """Define our VM, we require just name and ID"""
        self.name = name
        self.id = id
        self.ha = ha
        self.child_vms = []
        self.state = "" 
        self.max_memory = 0
        self.used_memory = 0
        self.cpus = 0
        self.cpu_usage = 0
        self.cost = {}

    def set_state(self, state):
        """Set the VM state, eg: "running", "stopped", etc"""
        self.state = state

    def set_memory_info(self, max_memory, used_memory):
        """Sets max usable and currently used memory"""
        self.max_memory = max_memory
        self.used_memory = used_memory

    def set_cpu_usage(self, cpus, cpu_usage):
        """Set the number of CPUs allocated and current usage"""
        self.cpus = cpus
        self.cpu_usage = cpu_usage

    def set_cost(self, cost):
        """Sets the as used by other classes"""
        self.cost = cost

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return str(self.name)


class ProxmoxHost():
    """Represents a Proxmox Host, we pass in the name and status
       and it keeps track of it's memory, cpu, running vms, etc"""
    # pylint: disable=too-many-instance-attributes
    def __init__(self, name, status):
        """Creates a new ProxmoxHost object"""
        self.name = name
        self.status = status
        self.child_vms = []
        # CPU info
        self.per_cpu_mhz = 0
        self.cpu_count = 0
        self.max_mhz = 0
        # Memory info
        self.max_memory = 0
        # Usage info
        self.allocated_memory = 0
        self.used_memory = 0
        self.used_cpu_mhz = 0
        self.running_vms = 0

    def set_cpu_info(self, cpu_speed, cpu_count):
        """Get the number of cpu cores, speed, and max usable mhz"""
        self.per_cpu_mhz = cpu_speed
        self.cpu_count = cpu_count
        self.max_mhz = cpu_count*cpu_speed

    def set_mem_info(self, max_memory):
        """Sets total memory"""
        self.max_memory = max_memory

    def get_usage(self):
        """Returns the total used and free CPU and Memory, and also the
           number of running VMs"""
        self.used_memory = 0
        self.used_cpu_mhz = 0
        self.running_vms = 0
        for VM in self.get_vms():
            self.allocated_memory += VM.max_memory
            self.used_memory += VM.used_memory
            self.used_cpu_mhz += VM.cpu_usage*(VM.cpus*self.per_cpu_mhz)
            if VM.state == "running":
                self.running_vms += 1
        free_cpu_mhz = self.max_mhz-self.used_cpu_mhz
        free_memory = self.max_memory-self.used_memory
        return {"used_cpu_mhz": self.used_cpu_mhz,
                "used_memory": self.used_memory,
                "allocated_memory": self.allocated_memory,
                "free_cpu_mhz": free_cpu_mhz,
                "free_memory": free_memory,
                "running_vms": self.running_vms}

    def add_vm(self, virtual_machine):
        """Adds a VM to the host and sets the cost of the VM"""
        vm_used_memory = virtual_machine.used_memory
        vm_cpu_mhz = virtual_machine.cpu_usage * \
            (virtual_machine.cpus*self.per_cpu_mhz)
        vm_cost = {"used_memory": vm_used_memory, "vm_cpu_mhz": vm_cpu_mhz}
        virtual_machine.set_cost(vm_cost)
        self.child_vms.append(virtual_machine)
        self.get_usage()

    def get_metric(self, metric):
        if metric == "used_memory":
            return self.used_memory
        if metric == "used_cpu_mhz":
            return self.used_cpu_mhz
        if metric == "running_vms":
            return self.running_vms
        return False

    def remove_vm(self, virtual_machine):
        self.child_vms.remove(virtual_machine)
        self.get_usage()

    def get_vms(self):
        return self.child_vms

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return str(self.name)


class ProxmoxCluster():

    # pylint: disable=too-many-instance-attributes
    def __init__(self, config, simulate=False):
        self.config = config
        self.simulate = simulate
        self.connected = False
        self.excluded_vms = self.config.get('exclude_vms', 'id').split(",")
        self.proxmox = None
        self.ha_vms = []
        self.maintenance_mode = []
        self.proxmox_nodes = []
        self.stats = {}

    def connect(self):
        try:
            self.proxmox = proxmoxer.ProxmoxAPI(self.config.get('proxmox', 'host'),
                                                user=self.config.get(
                                                    'proxmox', 'user'),
                                                password=self.config.get(
                                                    'proxmox', 'password'),
                                                verify_ssl=self.config.getboolean('proxmox', 'verify_ssl'))
            self.connected = True
        except e:
            print(e)
            sys.exit(1)

    def get_ha_vms(self):
        logger.debug("Finding VMs set up for HA")
        for ha_vm in self.proxmox.cluster.ha.resources.get():
            logger.debug("Found HA VM: %s", ha_vm['sid'])
            self.ha_vms.append(str(ha_vm['sid']).split(':')[1])

    def get_maintenance(self):
        for file in os.listdir(os.path.dirname(os.path.realpath(__file__))):
            if file.endswith(".maintenance"):
                self.maintenance_mode.append(file.replace(".maintenance", ""))

    def get_proxmox_nodes(self):
        self.get_maintenance()
        for node in self.proxmox.nodes.get():
            if node['status'] == 'offline':
                proxmox_node = ProxmoxHost(node['node'], "offline")
            else:
                if node['node'] in self.maintenance_mode:
                    proxmox_node = ProxmoxHost(node['node'], "maintenance")
                else:
                    proxmox_node = ProxmoxHost(node['node'], "online")
                proxmox_node.set_cpu_info(int(float(self.proxmox.nodes(
                    node['node']).status.get()['cpuinfo']['mhz'])), node['maxcpu'])
                proxmox_node.set_mem_info(node['maxmem'])
                self.proxmox_nodes.append(proxmox_node)

    def get_node(self, nodename):
        return [x for x in self.proxmox_nodes if x.name == nodename][0]

    def get_proxmox_vms(self, proxmox_node):
        for vm in self.proxmox.nodes(proxmox_node.name).qemu.get():
            logger.info("Found VM: %s (%s) on node %s",
                        vm['name'], vm['vmid'], proxmox_node)
            if vm['vmid'] in self.ha_vms:
                logger.debug("VM: %s (%s) has HA configuration",
                             vm['name'], vm['vmid'])
                proxmox_vm = ProxmoxVM(vm['name'], vm['vmid'], ha=True)
            else:
                proxmox_vm = ProxmoxVM(vm['name'], vm['vmid'])

            proxmox_vm.set_memory_info(vm['maxmem'], vm['mem'])
            logger.debug(vm)
            proxmox_vm.set_state(vm['status'])
            # Gets CPU usage in Mhz based on an average of the last hour:
            try:
                cpu_usage = []
                for x in self.proxmox.nodes(proxmox_node.name).qemu(vm['vmid']).rrddata.get(timeframe='hour'):
                    try:
                        cpu_usage.append(x['cpu'])
                    except KeyError:
                        pass
                # The values we get back are a percentage of how much CPU is being used
                # against what we've allocated. '1' is 100%. So '1' on a machine with 1 core
                # is almost nothing, while '1' on a machine with 24 cores is a bunch.
                #logger.debug("Got the following CPU history for %s (%s): %s", vm['name'], vm['vmid'], cpu_usage)
                cpu_usage = sum(cpu_usage)/float(len(cpu_usage))
            except ZeroDivisionError:
                logger.debug(
                    "Got ZeroDivisionError on %s (%s), it's probably turned off", vm['name'], vm['vmid'])
                cpu_usage = 0
                proxmox_vm.set_state("stopped")

            proxmox_vm.set_cpu_usage(vm['cpus'], cpu_usage)
            proxmox_node.add_vm(proxmox_vm)
            logger.debug("Finished processing VM: %s (%s)",
                         vm['name'], vm['vmid'])

    def get_cluster_info(self):
        self.get_ha_vms()
        self.get_proxmox_nodes()
        for proxmox_node in self.proxmox_nodes:
            self.get_proxmox_vms(proxmox_node)

    def summary(self):
        for node in self.proxmox_nodes:
            print(node, node.get_usage())
            for vm in node.get_vms():
                print("\t %s - %s" % (vm, vm.cost))

    def calculate_imbalances(self):
        unbalanced = False
        value_range = (.8, 1.2)
        metrics = ["used_memory", "used_cpu_mhz", "running_vms"]
        self.stats["average"] = {}
        for metric in metrics:
            self.stats["average"][metric] = int(
                statistics.mean([x.get_metric(metric) for x in self.proxmox_nodes]))
        for proxmox_node in self.proxmox_nodes:
            self.stats[proxmox_node.name] = {}
            for metric in metrics:
                if proxmox_node.get_metric(metric) > self.stats["average"][metric]*value_range[1]:
                    unbalanced = True
                    self.stats[proxmox_node.name][metric] = "HIGH"
                    self.stats[proxmox_node.name][metric+"_shed"] = proxmox_node.get_metric(metric) - \
                        self.stats["average"][metric]
                    logger.debug("Proxmox node %s has HIGH %s usage (%s > %s)",
                                 proxmox_node.name, metric, proxmox_node.get_metric(metric), self.stats["average"][metric]*value_range[1])
                elif proxmox_node.get_metric(metric) < self.stats["average"][metric]*value_range[0]:
                    self.stats[proxmox_node.name][metric] = "LOW"
                    self.stats[proxmox_node.name][metric+"_get"] = self.stats["average"][metric] - \
                        proxmox_node.get_metric(metric)
                    logger.debug("Proxmox node %s has LOW %s usage (%s < %s)",
                                 proxmox_node.name, metric, proxmox_node.get_metric(metric), self.stats["average"][metric]*value_range[0])
                else:
                    self.stats[proxmox_node.name][metric] = "OKAY"
        logger.debug(self.stats)
        return unbalanced



    def get_lowest_loaded(self, metric="cpu"):
        filtered_nodes = [
            x for x in self.proxmox_nodes if x.status == "online"]
        if metric == "cpu":
            return min(filtered_nodes, key=lambda x: x.used_cpu_mhz)
        if metric == "mem":
            return min(filtered_nodes, key=lambda x: x.used_memory)
        if metric == "vms":
            return min(filtered_nodes, key=lambda x: x.running_vms)
        return False

    def filter_candidates(self, vm_list, node, metric="cpu", minimum_pct=.75, maximum_pct=1.5, depth=0):
        if depth > 20:
            # 20 iterations is enough
            return None
        # First, we get all the machines that won't drain too much off of the host
        # but won't be a waste of time to move.
        if metric == "cpu":
            vm_candidates = [x for x in vm_list if x.cost['vm_cpu_mhz']/self.stats[node]["used_cpu_mhz_shed"] > minimum_pct
                             and x.cost['vm_cpu_mhz']/self.stats[node]["used_cpu_mhz_shed"] < maximum_pct]
        elif metric == "mem":
            vm_candidates = [x for x in vm_list if x.used_memory/self.stats[node]["used_memory_shed"] > minimum_pct
                             and x.used_memory/self.stats[node]["used_memory_shed"] < maximum_pct]
        else:
            vm_candidates = [
                min(vm_list, key=lambda x: x.used_memory+x.cost['vm_cpu_mhz']), ]
        logger.debug("VMs after candidate filtering: %s (min_pct=%s)",
                     vm_candidates, minimum_pct)
        if not vm_candidates:
            # No matches, reduce our shed by 10% and try again
            minimum_pct = minimum_pct*.90
            maximum_pct = maximum_pct*1.1
            vm_candidates = self.filter_candidates(
                vm_list, node, metric, minimum_pct=minimum_pct, maximum_pct=maximum_pct, depth=depth+1)
        try:
            if len(vm_candidates) == 1:
                virtual_machine_to_move = vm_candidates[0]
            else:
                # Next, we choose by grabbing the one with the lowest memory usage (if there are multiple)
                # Why lowest memory? Because those migrate faster, and with less inturruption to the underlying
                # OS
                virtual_machine_to_move = min(
                    vm_candidates, key=lambda x: x.used_memory)
        except TypeError:
            virtual_machine_to_move = vm_candidates
        logger.debug("Choosen VM: %s", virtual_machine_to_move)
        return virtual_machine_to_move

    def migration_planner(self):
        iterations = 0
        # A list of tuples ("Source host", "Virtual Machine to Move", "Target Host")
        planned_moves = []
        moving_vms = []
        maintenance_mode = [
            x for x in self.proxmox_nodes if x.status == "maintenance"]
        while self.calculate_imbalances2() and iterations < 100:
            iterations += 1
            proxmox_nodes_cpu_check = [
                x for x in self.stats if self.stats[x]["used_cpu_mhz"] == "HIGH"]
            proxmox_nodes_mem_check = [
                x for x in self.stats if self.stats[x]["used_memory"] == "HIGH"]
            proxmox_nodes_vm_check = [
                x for x in self.stats if self.stats[x]["running_vms"] == "HIGH"]
            # First things first, we need to check for maintenance mode and clear that system
            for node in maintenance_mode:
                logger.info("Want to move ALL off of %s", node)
                filtered_vms = [x for x in proxmox_node.child_vms if x not in moving_vms and x.state ==
                                "running" and x.id not in self.excluded_vms]
                while filtered_vms:
                    # We look for the host with the lowest memory first, as that tends to be the usual bottleneck
                    potential_host = self.get_lowest_loaded(metric="mem")
                    filtered_vms = [x for x in proxmox_node.child_vms if x not in moving_vms and x.state ==
                                    "running" and x.id not in self.excluded_vms]
                    # It doesn't really matter what order we do this in, but we start with the smallest
                    logger.debug(
                        "VMs not already being moved: %s", filtered_vms)
                    virtual_machine_to_move = min(filtered_vms)
                    logger.debug("Would probably move %s to: %s",
                                 virtual_machine_to_move.name, potential_host)
                    planned_moves.append(
                        (proxmox_node, virtual_machine_to_move, potential_host))
                    moving_vms.append(virtual_machine_to_move)
                    proxmox_node.remove_vm(virtual_machine_to_move)
                    potential_host.add_vm(virtual_machine_to_move)

            # And for memory. There's probably a much easier way to do this.
            for node in proxmox_nodes_mem_check:
                proxmox_node = self.get_node(node)
                logger.info("Want to move %s Memory units off of %s",
                            self.stats[node]["used_memory"], node)
                filtered_vms = [x for x in proxmox_node.child_vms if x not in moving_vms and x.state ==
                                "running" and x.id not in self.excluded_vms]
                potential_host = self.get_lowest_loaded(metric="mem")
                logger.debug("VMs not already being moved: %s", filtered_vms)
                if not filtered_vms:
                    continue
                virtual_machine_to_move = self.filter_candidates(
                    filtered_vms, node, metric="mem")
                if virtual_machine_to_move:
                    logger.debug("Would probably move %s (%s Memory units) to: %s",
                                 virtual_machine_to_move.name, virtual_machine_to_move.used_memory, potential_host)
                    logger.debug("%s - percentage of shed: %s",
                                 virtual_machine_to_move.name, (virtual_machine_to_move.used_memory/self.stats[node]["used_memory_shed"])*100)
                    planned_moves.append(
                        (proxmox_node, virtual_machine_to_move, potential_host))
                    moving_vms.append(virtual_machine_to_move)
                    proxmox_node.remove_vm(virtual_machine_to_move)
                    potential_host.add_vm(virtual_machine_to_move)

            # Now we check for overloaded CPUs
            for node in proxmox_nodes_cpu_check:
                proxmox_node = self.get_node(node)
                logger.info("Want to move %s CPU units off of %s",
                            self.stats[node]["used_cpu_mhz_shed"], node)
                potential_host = self.get_lowest_loaded(metric="cpu")
                filtered_vms = [x for x in proxmox_node.child_vms if x not in moving_vms and x.state ==
                                "running" and x.id not in self.excluded_vms]
                logger.debug("VMs not already being moved: %s", filtered_vms)
                if not filtered_vms:
                    continue
                virtual_machine_to_move = self.filter_candidates(
                    filtered_vms, node, metric="cpu")
                if virtual_machine_to_move:
                    logger.debug("Would probably move %s (%s CPU units) to: %s",
                                 virtual_machine_to_move.name, virtual_machine_to_move.cost['vm_cpu_mhz'], potential_host)
                    logger.debug("%s - percentage of shed: %s",
                                 virtual_machine_to_move.name, (virtual_machine_to_move.cost['vm_cpu_mhz']/self.stats[node]["used_cpu_mhz_shed"])*100)
                    planned_moves.append(
                        (proxmox_node, virtual_machine_to_move, potential_host))
                    moving_vms.append(virtual_machine_to_move)
                    proxmox_node.remove_vm(virtual_machine_to_move)
                    potential_host.add_vm(virtual_machine_to_move)


            # Lastly, purely cosmetic. Probably. We try to balance out the number of running VMs
            for node in proxmox_nodes_vm_check:
                proxmox_node = self.get_node(node)
                logger.info("Want to move %s VMs off of %s",
                            self.stats[node]["running_vms_shed"], node)
                filtered_vms = [x for x in proxmox_node.child_vms if x not in moving_vms and x.state ==
                                "running" and x.id not in self.excluded_vms]
                logger.debug("VMs not already being moved: %s", filtered_vms)
                if not filtered_vms:
                    continue
                potential_host = self.get_lowest_loaded(metric="vms")
                virtual_machine_to_move = self.filter_candidates(
                    filtered_vms, node, metric="vms")
                logger.debug("Would probably move %s to %s",
                             virtual_machine_to_move.name, potential_host)
                planned_moves.append(
                    (proxmox_node, virtual_machine_to_move, potential_host))
                moving_vms.append(virtual_machine_to_move)
                proxmox_node.remove_vm(virtual_machine_to_move)
                potential_host.add_vm(virtual_machine_to_move)

        return planned_moves

    def perform_migration_plan(self, planned_moves):
        logger.info("Planning the following moves: ")
        for move in planned_moves:
            source_host =  move[0]
            virtual_machine = move[1]
            destination_host = move[2]
            logger.info("MOVE: VM: %s, from %s to %s", virtual_machine, source_host, destination_host)
        for move in planned_moves:
            source_host =  move[0]
            virtual_machine = move[1]
            destination_host = move[2]
            if not self.simulate:
                logger.warning("Migrating VM %s from %s to %s",
                               virtual_machine,
                               source_host,
                               destination_host)
                try:
                    task_name = self.proxmox.nodes(source_host).qemu(
                        virtual_machine.id).migrate.post(target=destination_host, online=1)
                    logger.info(
                        "Migration of %s started with task ID of %s", virtual_machine, task_name)
                    while self.proxmox.nodes(source_host).tasks(task_name).get('status')['status'] != "stopped":
                        time.sleep(10)
                        logger.debug(
                            "Waiting for migration of %s -> %s to complete...", virtual_machine, destination_host)
                    exit_status = self.proxmox.nodes(source_host).tasks(
                        task_name).get('status')['exitstatus']
                    logger.info(
                        "Migration of %s to %s finished with status: %s", virtual_machine, destination_host, exit_status)
                except proxmoxer.core.ResourceException as e:
                    logger.error(
                        "Migration failed with an error: %s | Is another migration already in progress?", e)
            else:
                logger.warning("Would migrate VM %s from %s to %s",
                               virtual_machine, source_host, destination_host)

    def __str__(self):
        return str(self.proxmox_nodes)


def main():
    PID_FILE_LOC = '/var/run/proxmox_wlb.pid'
    PID_FILE = open(PID_FILE_LOC, 'w')
    try:
        fcntl.lockf(PID_FILE, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        # another instance is running
        print("Another instance is running, exiting...")
        sys.exit(0)

    config = configparser.RawConfigParser()
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", dest="config_file",
                        help="Where the config file is located")
    parser.add_argument("--simulate", dest="simulate", action='store_true',
                        help="Don't actually perform migrations")
    parser.add_argument("--no-simulate", dest="simulate", action='store_false',
                        help="Perform migrations")
    parser.add_argument("--debug", dest="debug", action='store_true',
                        help="Show debugging options")
    args = parser.parse_args()
    config.read(args.config_file)
    # Configure Logging
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    if args.debug:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.ERROR)
    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(ch)
    if config.get('main', 'logdir'):
        fh = logging.FileHandler(config.get(
            'main', 'logdir')+'/proxmox_wlb.log')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    proxmox = ProxmoxCluster(config, simulate=args.simulate)
    proxmox.connect()
    proxmox.get_cluster_info()
    proxmox.perform_migration_plan(proxmox.migration_planner())


if __name__ == "__main__":
    logger = logging.getLogger('proxmox_wlb')
    main()
