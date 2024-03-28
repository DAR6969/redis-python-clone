# Uncomment this to pass the first stage
import socket
import threading

class RedisProtocolParser:
    @staticmethod
    def parse(data):
        commands = data.decode().split('\r\n')[:-1]
        parsed_commands = []
        for cmd in commands:
            if cmd.startswith('*'):
                num_args = int(cmd[1:])
                parsed_args = []
                for _ in range(num_args):
                    cmd = commands.pop(0)
                    arg_len = int(cmd[1:])
                    arg = commands.pop(0)[:arg_len]
                    print(arg, "dhruv arg")
                    parsed_args.append(arg)
                parsed_commands.append(parsed_args)
        return parsed_commands
    
    def encode_redis_bulk_string(input_string):
        # Prefix the string with the length of the string and add the CRLF delimiter
        encoded_string = f"${len(input_string)}\r\n{input_string}\r\n"
        return encoded_string.encode()

def handleRequest(connection):
    pong = "+PONG\r\n"
    client_data = b""
    
    with connection:
        while True:
            data_stream = connection.recv(1024)
            if not data_stream:
                break
            print(data_stream)
            commands = RedisProtocolParser.parse(data_stream)
            if commands[0] == "ping":
                connection.send(pong.encode())
            elif commands[0] == "echo":
                response = RedisProtocolParser.encode_redis_bulk_string(commands[1])
                connection.send(response)
        connection.close()

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # server_socket.accept() # wait for client
    
    while True:
        connection, address = server_socket.accept()
        t1 = threading.Thread(target=handleRequest, args=(connection,),name="t1")
        
        t1.start()
    t1.join()


if __name__ == "__main__":
    main()
