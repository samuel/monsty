
import socket
import sys
import time
import yaml
from optparse import OptionParser

class CarbonClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None
    
    def connect(self):
        if not self.sock:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
    
    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None
    
    def send(self, points, timestamp=None, close=False):
        timestamp = str(timestamp or time.time())
        message = "\n".join(
            "%s %s %s" % (
                name, ("%.3f" % value) if isinstance(value, float) else value, timestamp
            ) for name, value in points.iteritems()
        )

        self.connect()
        self.sock.sendall(message)
        if close:
            self.close()

def build_parser():
    parser = OptionParser(usage="Usage: %prog [options] logfile")
    parser.add_option("-c", "--carbon", dest="carbon_addr", help="Address of crabon/graphite (host[:port])", default=None)
    parser.add_option("-d", "--dryrun", dest="dryrun", help="Print output instead of sending to crabon/graphite", default=False, action="store_true")
    return parser

def main():
    parser = build_parser()
    options, args = parser.parse_args()
    if not args:
        parser.error("must provide path of config file")
    
    with open(args[0], "r") as fp:
        config = yaml.load(fp)
    
    carbon_addr = config.get("carbon") or options.carbon_addr or "127.0.0.1:2003"
    if ":" not in carbon_addr:
        carbon_addr += ":2003"
    host, port = carbon_addr.split(':')
    port = int(port)
    
    if not options.dryrun:
        carbon = CarbonClient(host, port)
        carbon.connect()
    
    name_prefix = "rad"
    
    for pin in config["instruments"]:
        class_path = pin["class"]
        mod_path, class_name = class_path.rsplit('.', 1)
        mod = __import__(mod_path, {}, {}, [class_name])
        cls = getattr(mod, class_name)
        try:
            inst = cls(**pin["args"])
            info = inst.get_info()
        except:
            sys.stderr.write("Instrument %s failed\n" % pin["name"])
            import traceback
            traceback.print_exc()
        else:
            keys = pin.get('keys')
            info = dict(
                (".".join((name_prefix, pin["name"], k)), v)
                for k, v in inst.iteritems()
                if not keys or k in keys)
            if options.dryrun:
                import pprint
                pprint.pprint(info)
            else:
                carbon.send(info)
    
    if not options.dryrun:
        carbon.close()

if __name__ == "__main__":
    main()
