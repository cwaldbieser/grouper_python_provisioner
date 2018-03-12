##################
txgroupprovisioner
##################

.. highlight:: console

========
Overview
========

A Twisted Group Provisioner (*txgroupprovisioner*) is a service that
reads membership and account provisioning messages from an AMQP message 
queue and makes changes to a specific target service.

Provisioner services may provision memberships, or they may provision accounts,
or they may provision both memberships and accounts.  

The :term:`PDS` is a special kind of provisioner that
can accept messages from different kinds of sources, perfrom group and
attribute lookups, compose standard messages, and route them to the intended
provisioners. 

The general architecture for this provisioning system looks like a pipline
that flows from event sources to a provisioner delivery service and finally to
the provisioners.  There may be multiple pipelines.  For example, there may be
separate pipelines for membership provisioners and account provisioners.

