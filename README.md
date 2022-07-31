# Proxmox WLB
This is a load balancing script for Proxmox that allocates KVM containers between hosts to keep the load as symmetric as possible.

It supports putting hosts into maintenance mode and excluding VMs from being moved.

An example configuration is provided. Edit and move to "proxmox_wlb.cfg", then run the script with: `/root/proxmox/proxmox_wlb.py --config /root/proxmox/proxmox_wlb.cfg`

