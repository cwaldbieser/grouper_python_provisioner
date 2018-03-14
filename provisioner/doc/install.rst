
============
Installation
============

The Python requirements are listed in :file:`requirements.txt`.  The various shell scripts
assume that you have installed these in a Python virtual environment (ala `virtualenv`) in
the same folder called :file:`pyenv`.

Example::

    $ virtualenv pyenv
    $ . ./pyenv/bin/activate
    (pyenv)$ pip install -r ./requirements.txt


OS dependencies may vary slightly across distributions:

* python-devel
* libffi-devel

