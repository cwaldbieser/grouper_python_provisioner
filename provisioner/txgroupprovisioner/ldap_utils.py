
from ldaptor.protocols.ldap.distinguishedname import DistinguishedName, RelativeDistinguishedName

def escape_filter_chars(assertion_value,escape_mode=0):
    """
    This function shamelessly copied from python-ldap module.
    
    Replace all special characters found in assertion_value
    by quoted notation.

    escape_mode
      If 0 only special chars mentioned in RFC 2254 are escaped.
      If 1 all NON-ASCII chars are escaped.
      If 2 all chars are escaped.
    """
    if escape_mode:
        r = []
        if escape_mode==1:
            for c in assertion_value:
                if c < '0' or c > 'z' or c in "\\*()":
                    c = "\\%02x" % ord(c)
                r.append(c)
        elif escape_mode==2:
            for c in assertion_value:
                r.append("\\%02x" % ord(c))
        else:
          raise ValueError('escape_mode must be 0, 1 or 2.')
        s = ''.join(r)
    else:
        s = assertion_value.replace('\\', r'\5c')
        s = s.replace(r'*', r'\2a')
        s = s.replace(r'(', r'\28')
        s = s.replace(r')', r'\29')
        s = s.replace('\x00', r'\00')
    return s

def normalize_dn(dn):
    """
    Transforms a `DistinguishedName` into a `DistinguishedName` with the casing normalized.
    """
    return DistinguishedName(tuple(RelativeDistinguishedName(str(r).lower()) for r in dn.listOfRDNs))
