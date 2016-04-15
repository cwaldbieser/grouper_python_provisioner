
from twisted.plugin import getPlugins

def get_plugin_factory(tagname, iface, all_matches=False):
    """
    Get plugin factories for interface `iface` with tag names
    matching `tagname`.
    Returns the first match or None if no matches.
    If `all_matches` is set to True, return a list of matches or
    an empty list on no matches.
    """
    results = []
    for plugin in getPlugins(iface):
        if hasattr(plugin, 'tag') and plugin.tag == tagname:
            results.append(plugin)
            if not all_matches:
                break
    if not all_matches:
        if len(results) == 0:
            return None
        else:
            return results[0]
    else:
        return results

