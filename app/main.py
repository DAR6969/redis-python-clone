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
                    parsed_args.append(arg)
                parsed_commands.append(parsed_args)
        return parsed_commands

def handleRequest(connection):
    pong = "+PONG\r\n"
    client_data = b""
    
    with connection:
        while True:
            data_stream = connection.recv(1024)
            if not data_stream:
                break
            print(data_stream)
            connection.send(pong.encode())
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
