class CommonTools:
    my_local_port = ""
    ping = "PING"
    REPLCONF_port = "REPLCONF listening-port "
    REPLCONF_capa = "REPLCONF capa psync2"
    REPLCONF_ack = "REPLCONF ACK 0"
    psync = "PSYNC ? -1"
    master_host = ""
    master_port = 0
    replica_server = False
    slaves = {}
    replica_backlog = []
    get_map = {}
    replica_port = ""
    received_replica_handshake = False
    pong = "+PONG\r\n"
    ok = "+OK\r\n"
    null_bulk = "$-1\r\n"
        
    def __init__(self) -> None:
        pass

    def set_my_port(self, port):
        CommonTools.my_local_port = str(port)
        self.set_REPLCONF_port()
        
    def set_REPLCONF_port(self):
        CommonTools.REPLCONF_port += CommonTools.my_local_port
    
    def set_master_addr(self, host, port):
        CommonTools.master_host = host
        CommonTools.master_port = int(port)
    
    def set_replica_server(self, value: bool):
        CommonTools.replica_server = value
    
    
    

    
    
    