class CommonTools:
    my_local_port = ""
    ping = "PING"
    REPLCONF_port = "REPLCONF listening-port "
    REPLCONF_capa = "REPLCONF capa psync2"
    psync = "PSYNC ? -1"
    master_host = ""
    master_port = 0
        
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

    
    
    