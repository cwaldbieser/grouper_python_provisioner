
=======
Running
=======

A single instance of a provisioner may be invoked as a twisted plugin::

    $ ./twistd.sh --syslog provisioner

Other options for the `provisioner` plugin or the `twistd` program itself
are available.  Try using the `--help` option for more information.

.. note::

    The architecture of the txgroupprovisioner system is that the message
    exchange forms a backbone that connects event sources to provisioners.
    As such, there isn't a single `run` command for the entire system.
    Instead, individual services can be stopped and started.

-------------------------------
OS Service Integration examples
-------------------------------

"""""""""""""
RHEL6 Upstart
"""""""""""""

The following is an example of a simple Upstart script to daemonize an LDAP
subject provisioner service.

.. code-block:: shell
    :linenos:

    description "Twisted LDAP Subject Provisioner"
    author "Carl <waldbiec@lafayette.edu>"

    start on runlevel [2345]
    stop on runlevel [016]

    kill timeout 60
    respawn
    respawn limit 10 5
     
    script
    /usr/bin/sudo -u grouper /bin/bash <<START
    cd /opt/txgroupprovisioner/ 
    . ./pyenv/bin/activate 
    export LD_LIBRARY_PATH=/usr/local/lib64
    ./twistd.sh -n --syslog --prefix ldapsubj --pidfile /var/run/txgroupprovisioner/ldapsubj.pid  provisioner -c /etc/grouper/provisioners/ldapsubj.cfg
    START
    end script

The majority of the configuration is the Upstart boilerplate.  The actual
script block is a shell HERE document from lines 13-16.  The script is
run as the *grouper* user on the system using the :program:`sudo` command.

On line 13, the current directory is changed to the software installation
folder.  On line 14, the Python virtual environment for the software is
activated.  Line 15 sets up an environment variable- in this case the
:envvar:`LD_LIBRARY_PATH` variable lets the operating system know that it needs
to look in a non-standard location for a shared library the software depends
on.

Line 16 is where the service is finally invoked.  In this case, the `-n`
option is used to run the service in the foreground.  The Upstart machinery will
take care of running the process as a background service.  Other options are
provided to log to syslog using a particular prefix, choose a file to use for
tracking the process PID, and specifying an individual configuration file.

