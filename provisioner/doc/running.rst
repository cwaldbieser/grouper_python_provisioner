
=======
Running
=======

A single instance of the provisioner may be invoked as a twisted plugin::

    $ ./twistd.sh --syslog provisioner

Other options for the `provisioner` plugin or the `twistd` program itself
are available.  Try using the `--help` option for more information.

Alternatively, specific configurations for multiple provisioners may be placed
in a `conf.d` folder in the main application folder.  The scripts `provision`
and `stop-provisioning` can be used to collectively start and stop multiple configured
provisioners.

