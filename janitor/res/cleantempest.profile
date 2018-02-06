[sweeper]
# Announcement
banner = "Starting tempest/rally cleaning."

# sort sections before use
presort_sections = True

# default regex filter
common_filter = .*(rally_|tempest_|tempest-).*$

# retry attempts on failure
retry = 3
timeout = 2000

# concurrency for sweep action, not implemented
concurrency = 1

# additional script/commands, not implemented
pre_script = $(export OS_ENDPOINT_TYPE=internal)
post_script =

# default output format (raw, json)
default_format_parser = json

# default field for filtering
default_filter_field = name

# Run next section by default only if all previous was successful
default_protected_run = False

############
# [examplesection1]
# if action_map is set, section pre-builds data tree according to map
# and links it by selected key. '*' means whole line will be saved as argument
# Action will follow data tree and execute each action from leaves and up
# action_map = name1.name2
# name1_list_action = find . -maxdepth 1 -type d
# name2_list_action = find . -maxdepth 1 -type f
# name1_key = *
# name2_key = *
# name1_sweep_action = file {}
# name2_sweep_action = file {}

# bash command to get list of objects to clean, no quotes
# list_action = find . -maxdepth 1 -type f

# openstack client has table heading, use this to parse data to dict
key = ID

# bash command for action on single object, brackets to mark argument placement
# use indices if you place same argument multiple times, just like in Python :)
# sweep_action = file {}
# sweep_action = file {0}; tail {0}
############

[01.Users]
# Remove users
list_action = openstack user list
key = ID
sweep_action = openstack user delete {}

[02.Roles]
# Remove roles
list_action = openstack role list
key = ID
sweep_action = openstack role delete {}

[03.Service]
# Remove services
list_action = openstack service list
key = ID
sweep_action = openstack service delete {}

[04.Servers]
# Remove created instances
list_action = openstack server list --all
key = ID
sweep_action = openstack server delete {}

[05.Snapshots]
# Remove created snapshots
list_action = cinder snapshot-list --all
key = ID
sweep_action = cinder snapshot-reset-state {0}; cinder snapshot-delete {0} --force

[06.Volumes]
# Remove created volumes
list_action = openstack volume list --all
key = ID
sweep_action = cinder reset-state {0}; openstack volume delete {0}

[07.VolumeTypes]
# Remove created volume types
list_action = cinder type-list
key = ID
sweep_action = cinder type-delete {}

[08.Images]
# Remove created images
list_action = openstack image list
key = ID
sweep_action = openstack image delete {}

[09.SecurityGroups]
# Remove created Security Groups
list_action = openstack security group list --all
key = ID
sweep_action = openstack security group delete {}

[10.KeyPairs]
# Remove created SSH key pairs
list_action = openstack keypair list
key = ID
sweep_action = openstack keypair delete {}

[11.Networks]
# Remove created networks, and its subsidiaries
action_map = network.subnet.port

network_list_action = openstack network list
network_key = ID
network_sweep_action = openstack network delete {}
network_child_list = subnet,port

subnet_list_action = openstack subnet list
subnet_key = ID
subnet_sweep_action = openstack subnet delete {}
subnet_child_options = --network {}, params.network.ID

port_list_action = openstack port list
port_key = ID
port_sweep_action = openstack port delete {}
port_child_options = --network {}, params.network.ID

[12.Routers]
# Remove created routers
list_action = openstack router list
key = ID
sweep_action = openstack router delete {}

[13.Regions]
# Remove created regions
list_action = openstack region list
key = ID
sweep_action = openstack region delete {}

[14.Stacks]
# Remove created heat stacks, include nested
list_action = openstack stack list --nested
key = ID
sweep_action = openstack stack delete -y {}

[15.Containers]
# Remove any test containers
list_action = openstack container list --all
key = ID
sweep_action = openstack container delete {}

[16.Projects]
# Remove projects, run only if all others were successful
protected_run = True
list_action = openstack project list
key = ID
sweep_action = openstack project delete {}
