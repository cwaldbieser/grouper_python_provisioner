
=================
REST Provisioners
=================

REST provisioners are a class of provisioner that derive from the base
:py:class:`txgroupprovisioner.rest_provisioner.RestProvisioner`.  These
provisioners share many similar traits and options.

Each REST provisioner uses an HTTP-based API in order to query and manipulate
a target system.  Many APIs claim to be RESTful, though strictly speaking this
is not a requirement.

* account_sync_rate_limit_ms
* member_sync_rate_limit_ms
* provision_strategy ("query-first", "create-first")
* group_sync_strategy ("add-member-first", "query-first")

