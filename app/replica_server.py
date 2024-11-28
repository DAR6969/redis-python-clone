import socket
import time

from app.common_file import CommonTools
from app.RedisParser import RedisProtocolParser

class ReplicaServer:
    
    def __init__(self) -> None:
        pass
        
    def listen_to_master(self):
        common_tools = CommonTools()
        parser = RedisProtocolParser()
        print("hello")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((common_tools.master_host, common_tools.master_port))
        
        print("dhruv new socket connected")
        self.sock.send(RedisProtocolParser.create_array(common_tools.ping))
        
        
        response = self.sock.recv(1024)
        print(f"{response.decode()}, dhruv new replica self.socket response from master 1")
        
        self.sock.send(RedisProtocolParser.create_array(*common_tools.REPLCONF_port.split()))
        response = self.sock.recv(1024)
        print(f"{response.decode()}, dhruv new replica self.socket response from master 2")
        
        self.sock.send(RedisProtocolParser.create_array(*common_tools.REPLCONF_capa.split()))
        response = self.sock.recv(1024)
        print(f"{response.decode()}, dhruv new replica self.socket response from master 3")
        
        self.sock.send(RedisProtocolParser.create_array(*common_tools.psync.split()))
        # full resync
        # response = self.sock.recv(1024)
        # print(response, "master rdb response")
        # time.sleep(1)
        
        # # rdb file
        # response = self.sock.recv(1024)
        # print(response, "master full rdb file")
        # time.sleep(1)
        
        # response = self.sock.recv(1024)
        # print(RedisProtocolParser.parse(response) , "check set commands new")
        
        while True:
            try:
                msg = self.sock.recv(1024)
                print(msg, "received message")
                print(parser.feed(msg), "master sent message loop")
                if not msg:
                    print("Connection closed by the master")
                    break
                print(f"Received Message from master")
            except Exception as e:
                print(f"Error while receiving message on replicaY: {e}")
                break   