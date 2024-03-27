# Uncomment this to pass the first stage
import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # server_socket.accept() # wait for client
    
    pong = "+PONG\r\n"
    connection, address = server_socket.accept()
    with connection:
        for i in range(0,2):
            print("hello", i)
            connection.recv(1024)
            connection.send(pong.encode())
            
        


if __name__ == "__main__":
    main()
