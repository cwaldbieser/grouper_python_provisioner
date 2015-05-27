
from ConfigParser import SafeConfigParser
from  cStringIO import StringIO
import os.path

def load_config(config_file=None, defaults=None):
    basefile="txgroupprovisioner.cfg"
    syspath = os.path.join("/etc/grouper", basefile)
    homepath = os.path.expanduser("~/.{0}".format(basefile))
    apppath = os.path.join(os.path.dirname(__file__), basefile)
    curpath = os.path.join(os.curdir, basefile)
    files = [syspath, homepath, apppath, curpath]
    if config_file is not None:
        files.append(config_file)
    scp = SafeConfigParser()
    if defaults is not None:
        buf = StringIO(defaults)
        scp.readfp(buf)
    scp.read(files)
    return scp
    
def section2dict(scp, section):
    d = {}
    for option in scp.options(section):
        d[option] = scp.get(section, option)
    return d
