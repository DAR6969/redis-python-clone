# Uncomment this to pass the first stage
import socket
import threading
import time
import argparse 

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
            bulk_string += f"${len(arg)}\r\n{arg}\r\n"
        return bulk_string.encode()

get_map = {}

def remove_key_px(key, delay):
    # time.sleep(delay)  # Sleep for the specified delay
    if key in get_map:
        del get_map[key]
    print(get_map, "dhruv map set 2")


def handleRequest(connection):
    pong = "+PONG\r\n"
    ok = "+OK\r\n"
    null_bulk = "$-1\r\n"
    client_data = b""
    
    with connection:
        while True:
            data_stream = connection.recv(1024)
            if not data_stream:
                break
            # print(data_stream, "dhruv stream")
            commands = RedisProtocolParser.parse(data_stream)
            print(commands, "dhruv commands")
            if commands[0] == "ping":
                connection.send(pong.encode())
            elif commands[0][0] == "echo":
                response = RedisProtocolParser.encode_redis_bulk_string(commands[0][1])
                # print(response, "dhruv resp")
                connection.send(response)
            elif commands[0][0] == "set":
                get_map[commands[0][1]] = commands[0][2]
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
                    print("replicate comm sent")
                    connection.send("$10\r\nrole:slave\r\n".encode())    
                else:
                    print("master comm sent")
                    # master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                    # master_repl_offset = 0
                    # new_info_result = RedisProtocolParser.create_bulk_string("role:master",master_replid, master_repl_offset)
                    # connection.send(new_info_result)
                    connection.send("$11\r\nrole:master\r\n".encode())
                    
        connection.close()

def parse_arguments():
    parser = argparse.ArgumentParser(description="Decode flag arguments for TCP connection command")
    parser.add_argument('--port', type=int, help='Port number to connect', required=False, default=6379, dest='port')
    parser.add_argument('--replicaof', nargs='+', help='Port number to connect', required=False, dest='master')
    
    return parser.parse_args()


replica_server = False

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
    
    while True:
        connection, address = server_socket.accept()
        t1 = threading.Thread(target=handleRequest, args=(connection,),name="t1")
        
        t1.start()
    t1.join()


if __name__ == "__main__":
    main()
