import socket
import time

import app.CommandHelper as cmd_helper

from app.common_file import CommonTools
from app.RedisParser import RedisProtocolParser
from app.server_op import handle_commands_server

class ReplicaListener:
    
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
        
        handle_commands_server(self.sock, server_arg=False)
        
        # while True:
        #     try:
        #         msg = self.sock.recv(1024)
        #         print(msg, "received message")
        #         command = parser.feed(msg)
        #         print(command, "master sent message loop")
        #         cmd_helper.replica_set(command)
        #         if not msg:
        #             print("Connection closed by the master")
        #             break
        #         print(f"Received Message from master")
        #     except Exception as e:
        #         print(f"Error while receiving message on replica new: {e}")
        #         break   