# Uncomment this to pass the first stage
import socket
import threading
import time
import argparse 
import base64

class RedisProtocolParser:
    @staticmethod
    def parse(data):
        commands = data.decode().split('\r\n')[:-1]
        # print(commands, "dhruv function commands")
        parsed_commands = []
        if len(commands) == 3 and commands[0] == '*1' and commands[1].startswith('$'):
            arg_len = int(commands[1][1:])
            arg = commands[2][:arg_len]
            parsed_commands.append(arg)
        else:    
            commands_copy = commands.copy()
            for cmd in commands_copy:
                if cmd.startswith('*'):
                    num_args = int(cmd[1:])
                    parsed_args = []
                    commands.pop(0)
                    for _ in range(num_args):
                        cmd_new = commands.pop(0)
                        # print(cmd_new, "dhruv new")
                        # print(commands, "dhruv commads pop 1")
                        arg_len = int(cmd_new[1:])
                        arg = commands.pop(0)[:arg_len]
                        # print(commands, "dhruv commads pop 2")
                        # print(arg, "dhruv arg")
                        parsed_args.append(arg)
                    parsed_commands.append(parsed_args)
        return parsed_commands
    
    def encode_redis_bulk_string(input_string):
        # Prefix the string with the length of the string and add the CRLF delimiter
        encoded_string = f"${len(input_string)}\r\n{input_string}\r\n"
        return encoded_string.encode()
    
    def create_bulk_string(*args):
        bulk_string = ""
        for arg in args:
            bulk_string += f"${len(str(arg))}\r\n{arg}\r\n"
        return bulk_string.encode()
    
    def create_bulk_string_bytes(input_bytes):
        length = len(input_bytes)
        encoded_string = f"${length}\r\n".encode()
        encoded_string += input_bytes
        # encoded_string += "\r\n"
        return encoded_string
    
    def create_array(*args):
        num_elements = len(args)
        array_string = f"*{num_elements}\r\n"
        for arg in args:
            if isinstance(arg, int):
                arg = str(arg)
            array_string += f"${len(arg)}\r\n{arg}\r\n"
        return array_string.encode()

get_map = {}

def remove_key_px(key, delay):
    # time.sleep(delay)  # Sleep for the specified delay
    if key in get_map:
        del get_map[key]
    print(get_map, "dhruv map set 2")

replica_server = False
# global received_replica_handshake
received_replica_handshake = False
replica_backlog = []

def handleRequest(connection):
    pong = "+PONG\r\n"
    ok = "+OK\r\n"
    null_bulk = "$-1\r\n"
    client_data = b""
    
    with connection:
        while True:
            data_stream = connection.recv(1024)
            if not data_stream:
                global replica_backlog
                if(len(replica_backlog) >= 3):
                    print("dhruv inside new", len(replica_backlog))   
                    for command in replica_backlog:
                        print(command, "dhruv prop command")
                        connection.send(command)
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
                    rep_command = RedisProtocolParser.create_bulk_string(*commands[0])
                    connection.send(ok.encode())
                    # connection.send(rep_command)
                    # global replica_backlog
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
                connection.send(ok_response.encode())
            elif commands[0][0] == "PSYNC":
                if not replica_server:
                    master_replid = f"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                    sync_res = "+FULLRESYNC " + master_replid + " 0\r\n"
                    connection.send(sync_res.encode())
                    
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

    # Uncomment this to pass the first stage
    #
    args = parse_arguments()
    master = args.master
    if master is not None:
        global replica_server
        replica_server = True
    print(master, "dhruv masters")
    print(replica_server, "dhruv replica")
        

    # Extract and print flag values if provided
    if args.port is not None:
        port = args.port
        print(f"Port number: {port}")
    else:
        port = 6379
        print("Port number not specified.")
        
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    # server_socket.accept() # wait for client
    if master is not None:
        ping = "PING"
        REPLCONF_port = "REPLCONF listening-port " + str(args.port)
        REPLCONF_capa = "REPLCONF capa psync2"
        psync = "PSYNC ? -1"
        # handshake_messages = ["PING", "REPLCONF listening-port <PORT>"]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((master[0], int(master[1])))
        sock.send(RedisProtocolParser.create_array(ping))
        response = sock.recv(1024)
        print(f"{response.decode()}, dhruv replica socket response from master 1")
        sock.send(RedisProtocolParser.create_array(*REPLCONF_port.split()))
        response = sock.recv(1024)
        print(f"{response.decode()}, dhruv replica socket response from master 2")
        sock.send(RedisProtocolParser.create_array(*REPLCONF_capa.split()))
        response = sock.recv(1024)
        print(f"{response.decode()}, dhruv replica socket response from master 3")
        sock.send(RedisProtocolParser.create_array(*psync.split()))
        response = sock.recv(1024)
        print(f"{response.decode()}, dhruv replica socket response from master 4")
    
    while True:
        connection, address = server_socket.accept()
        print("connection received to server")
        t1 = threading.Thread(target=handleRequest, args=(connection,),name="t1")
        
        t1.start()
    t1.join()


if __name__ == "__main__":
    main()
