
import socket

class RedisInstrument(object):
    DEFAULT_INFO_KEYS = (
        "used_cpu_sys",
        "used_cpu_user",
        "connected_clients",
        "connected_slaves",
        "blocked_clients",
        "used_memory",
        "used_memory_rss",
        "mem_fragmentation_ratio",
        "total_connections_received",
        "total_commands_processed",
        "expired_keys",
        "evicted_keys",
        "keyspace_hits",
        "keyspace_misses",
        "pubsub_channels",
        "pubsub_patterns",
    )
    
    def __init__(self, host, port=6379):
        self.host = host
        self.port = port

    def get_info(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        s.send("*1\r\n$4\r\ninfo\r\n")
        buf = ""
        while '\r\n\r\n' not in buf:
            buf += s.recv(1024)
        s.close()

        info = dict(x.split(':', 1) for x in buf.split('\r\n')[1:-2] if not x.startswith("#") and ":" in x)
        return dict((key, info[key]) for key in self.DEFAULT_INFO_KEYS if key in info)
