#############
txgroupserver
#############

The Twisted Group Server (*txgroupserver*) accepts HMAC verified messags about group membership
changes from a Grouper change log consumer.  It batches the requests and periodically updates
and OpenLDAP directory server with the changes.

============
Installation
============
The Python requirements are listed in :file:`requirements.txt`.  The variaous shell scripts
assume that you have installed these in a Python virtual environment (ala `virtualenv`) in
the same folder called :file:`pyenv`.

OS dependencies may vary slightly across distributions:

* python-devel
* openldap-devel

Probably:

* libffi-devel

