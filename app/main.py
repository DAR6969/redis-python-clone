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
from app.replica_server import ReplicaServer
from app.RedisParser import RedisProtocolParser

# class RedisProtocolParser:
#     @staticmethod
#     def parse(data):
#         commands = data.decode().split('\r\n')[:-1]
#         print(commands, "dhruv function commands")
#         parsed_commands = []
#         if len(commands) == 3 and commands[0] == '*1' and commands[1].startswith('$'):
#             arg_len = int(commands[1][1:])
#             arg = commands[2][:arg_len]
#             parsed_commands.append(arg)
#         else:    
#             commands_copy = commands.copy()
#             for cmd in commands_copy:
#                 if cmd.startswith('*'):
#                     num_args = int(cmd[1:])
#                     parsed_args = []
#                     commands.pop(0)
#                     for _ in range(num_args):
#                         cmd_new = commands.pop(0)
#                         # print(cmd_new, "dhruv new")
#                         # print(commands, "dhruv commads pop 1")
#                         arg_len = int(cmd_new[1:])
#                         arg = commands.pop(0)[:arg_len]
#                         # print(commands, "dhruv commads pop 2")
#                         # print(arg, "dhruv arg")
#                         parsed_args.append(arg)
#                     parsed_commands.append(parsed_args)
#         return parsed_commands
    
#     def encode_redis_bulk_string(input_string):
#         # Prefix the string with the length of the string and add the CRLF delimiter
#         encoded_string = f"${len(input_string)}\r\n{input_string}\r\n"
#         return encoded_string.encode()
    
#     def create_bulk_string(*args):
#         bulk_string = ""
#         for arg in args:
#             bulk_string += f"${len(str(arg))}\r\n{arg}\r\n"
#         return bulk_string.encode()
    
#     def create_bulk_string_bytes(input_bytes):
#         length = len(input_bytes)
#         encoded_string = f"${length}\r\n".encode()
#         encoded_string += input_bytes
#         # encoded_string += "\r\n"
#         return encoded_string
    
#     def create_array(*args):
#         num_elements = len(args)
#         array_string = f"*{num_elements}\r\n"
#         for arg in args:
#             if isinstance(arg, int):
#                 arg = str(arg)
#             array_string += f"${len(arg)}\r\n{arg}\r\n"
#         return array_string.encode()

get_map = {}

def remove_key_px(key, delay):
    # time.sleep(delay)  # Sleep for the specified delay
    if key in get_map:
        del get_map[key]
    print(get_map, "dhruv map set 2")

replica_server = False
# global received_replica_handshake
received_replica_handshake = False
replica_port = ""
replica_backlog = []
slaves = {}

def handleRequest(connection, address):
    pong = "+PONG\r\n"
    ok = "+OK\r\n"
    null_bulk = "$-1\r\n"
    client_data = b""
    
    with connection:
        while True:
            data_stream = connection.recv(1024)
            global replica_port
            global replica_backlog
            global slaves
            if not data_stream:
                # if(len(replica_backlog) >= 3):
                # # if(len(slaves) > 0):
                    
                #     print("dhruv inside new", len(replica_backlog))   
                #     for command in replica_backlog:
                #         print(command, "dhruv prop command")
                #         connection.send(command)
                break
            # print(data_stream, "dhruv stream")
            commands = RedisProtocolParser.parse(data_stream)
            print(commands, "dhruv commands")
            if commands[0] == "ping" or commands[0] == "PING":
                connection.send(pong.encode())
            elif commands[0][0] == "echo":
                response = RedisProtocolParser.encode_redis_bulk_string(commands[0][1])
                # print(response, "dhruv resp")
                connection.send(response)
            elif commands[0][0] == "set" or commands[0][0] == "SET" :
                get_map[commands[0][1]] = commands[0][2]
                
                global received_replica_handshake
                if(received_replica_handshake):
                    rep_command = RedisProtocolParser.create_array(*commands[0])
                    connection.send(ok.encode())
                    # connection.send(rep_command)
                    # global replica_backlog
                    if len(slaves) > 0:
                        for address in slaves.keys():
                            if(slaves[str(address)]["connected"]):
                                try:
                                    print("propogating comm to slave")
                                    print(slaves, "druv slave list")
                                    # print(connection, "dhruv connection actual")
                                    slaves[str(address)]["connection"].sendall(rep_command)
                                except Exception as e:
                                    print("Error:", e)
                                    
                    replica_backlog.append(rep_command)
                else: 
                    connection.send(ok.encode())
                if(len(commands[0])>3 and commands[0][3] == "px"):
                    print("hello!")
                    key_remove = commands[0][1]
                    delay = int(commands[0][4])/1000
                    timer = threading.Timer(delay, remove_key_px, args=(key_remove, delay))
                    timer.start()
                print(get_map, "dhruv map set")
            elif commands[0][0] == "get":
                print(get_map, "dhruv map get")
                #propogate to slaves
                if(received_replica_handshake):
                    rep_command = RedisProtocolParser.create_array(*commands[0])
                    connection.send(ok.encode())
                    # connection.send(rep_command)
                    # global replica_backlog
                    if len(slaves) > 0:
                        for address in slaves.keys():
                            if(slaves[str(address)]["connected"]):
                                try:
                                    print("propogating comm to slave")
                                    print(slaves, "druv slave list")
                                    print(connection, "dhruv connection actual")
                                    slaves[str(address)]["connection"].sendall(rep_command)
                                except Exception as e:
                                    print("Error:", e)
                if commands[0][1] in get_map:
                    response = RedisProtocolParser.encode_redis_bulk_string(get_map[commands[0][1]])
                else:
                    response = null_bulk.encode()
                connection.send(response) 
            elif commands[0][0] == "INFO":
                if replica_server:
                    print("replicate comm recieved")
                    connection.send("$10\r\nrole:slave\r\n".encode())    
                else:
                    print("master comm received")
                    role=f"role:master"
                    master_replid = f"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                    master_repl_offset = f"master_repl_offset:0"
                    new_string = role+master_replid+master_repl_offset
                    # role_info_result = RedisProtocolParser.create_bulk_string("role:master")
                    new_info_result = RedisProtocolParser.create_bulk_string(new_string)
                    print(new_info_result, "dhruv new result")
                    connection.send(new_info_result)
                    # connection.send("$11\r\nrole:master\r\n".encode())
            elif commands[0][0] == "REPLCONF":   
                ok_response = "+OK\r\n"
                if(commands[0][1] == "listening-port"):
                    replica_port = commands[0][2]
                    slaves[str(address)] = {
                        "port": replica_port,
                        "connection": connection,
                        "connected": False,
                    }
                connection.send(ok_response.encode())
            elif commands[0][0] == "PSYNC":
                if not replica_server:
                    master_replid = f"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                    sync_res = "+FULLRESYNC " + master_replid + " 0\r\n"
                    connection.send(sync_res.encode())
                    # print(address, "address")
                    slaves[str(address)]["connected"] = True
                    
                    empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
                    # length_rdb = len(empty_rdb_base64)
                    empty_rdb_bytes = base64.b64decode(empty_rdb_base64)
                    empty_rdb_binary = RedisProtocolParser.create_bulk_string_bytes(empty_rdb_bytes)
                    print(empty_rdb_binary, "Dhruv binary 3 new")
                    connection.send(empty_rdb_binary)
                    # global received_replica_handshake
                    received_replica_handshake = True
            print(len(replica_backlog), "dhruv length backlog new 1")
            
        connection.close()

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
        global replica_server
        replica_server = True
        master = master[0].split()
        print(master)
        common_tools.set_master_addr(master[0], master[1])
        print(master, "dhruv masters")
        print(replica_server, "dhruv replica")
        

    # Extract and print flag values if provided
    # assign port based on replica or master 
    if args.port is not None:
        common_tools.set_my_port(args.port)
        print(f"Port number: {common_tools.my_local_port}")
    else:
        common_tools.set_my_port(6379)
        print("Port number not specified, going with 6379.")

    if master is not None:
        replica_handshake = ReplicaServer()
        threading.Thread(target=replica_handshake.listen_to_master, daemon=True).start()
    
    # create my own server (I could be master or replica) 
    # that will listen to connections from clients (could be replicas or other clients)  
    print("server socket reached")
    server_socket = socket.create_server(("localhost", int(common_tools.my_local_port)), reuse_port=True)
    while True:
        connection, address = server_socket.accept()
        t1 = threading.Thread(target=op.handle_commands_server, args=(connection, address),name="t1")
        t1.start()
    t1.join()


if __name__ == "__main__":
    main()
