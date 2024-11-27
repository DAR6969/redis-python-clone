class CommonTools:
    def __init__(self) -> None:
        my_local_port = ""
        ping = "PING"
        REPLCONF_port = "REPLCONF listening-port "
        REPLCONF_capa = "REPLCONF capa psync2"
        psync = "PSYNC ? -1"
        master_host = ""
        master_port = 0

    def set_my_port(self, port):
        self.my_local_port = str(port)
        self.set_REPLCONF_port()
        
    def set_REPLCONF_port(self):
        self.REPLCONF_port += self.my_local_port
    
    def set_master_addr(self, host, port):
        self.master_host = host
        self.master_port = int(port)

    
    
    