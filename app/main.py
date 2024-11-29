# Uncomment this to pass the first stage
import socket
import threading
import time
import argparse 
import base64

import os
import sys

import app.server_op as op

print("Current Working Directory:", os.getcwd())
print("Python Path:", sys.path)

from app.common_file import CommonTools
from app.replica_handshake import ReplicaListener
from app.RedisParser import RedisProtocolParser


def parse_arguments():
    parser = argparse.ArgumentParser(description="Decode flag arguments for TCP connection command")
    parser.add_argument('--port', type=int, help='Port number to connect', required=False, default=6379, dest='port')
    parser.add_argument('--replicaof', nargs='+', help='Port number to connect', required=False, dest='master')
    
    return parser.parse_args()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    
    common_tools = CommonTools()

    # Uncomment this to pass the first stage
    #
    args = parse_arguments()
    master = args.master
    if master is not None:
        CommonTools.set_replica_server(True)
        master = master[0].split()
        print(master)
        common_tools.set_master_addr(master[0], master[1])
        print(master, "dhruv masters")
        print(common_tools.replica_server, "dhruv spawn replica server")
        

    # Extract and print flag values if provided
    # assign port based on replica or master 
    if args.port is not None:
        common_tools.set_my_port(args.port)
        print(f"Port number: {CommonTools.my_local_port}")
    else:
        common_tools.set_my_port(6379)
        print("Port number not specified, going with 6379.")

    if master is not None:
        replica_handshake = ReplicaListener()
        threading.Thread(target=replica_handshake.listen_to_master, daemon=True).start()
    
    # create my own server (I could be master or replica) 
    # that will listen to connections from clients (could be replicas or other clients)  
    print("server socket reached")
    server_socket = socket.create_server(("localhost", int(CommonTools.my_local_port)), reuse_port=True)
    while True:
        connection, address = server_socket.accept()
        t1 = threading.Thread(target=op.handle_commands_server, args=(connection, address),name="t1")
        t1.start()
    t1.join()


if __name__ == "__main__":
    main()
