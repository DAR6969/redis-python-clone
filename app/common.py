class CommonTools:
    def __init__(self) -> None:
        self.my_local_port = ""
        self.ping = "PING"
        self.REPLCONF_port = "REPLCONF listening-port "
        self.REPLCONF_capa = "REPLCONF capa psync2"
        self.psync = "PSYNC ? -1"
        self.master_host = ""
        self.master_port = ""

    def set_my_port(self, port):
        self.my_local_port = str(port)
        self.set_REPLCONF_port()
        
    def set_REPLCONF_port(self):
        self.REPLCONF_port += self.my_local_port
    
    def set_master_addr(self, host, port):
        self.master_host = host
        self.master_port = int(port)
    
    
    