# Uncomment this to pass the first stage
import socket
import threading


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # server_socket.accept() # wait for client
    
    pong = "+PONG\r\n"
    client_data = b""
    
    connection, address = server_socket.accept()
    with connection:
        while True:
            data_stream = connection.recv(1024)
            connection.send(pong.encode())
            
        


if __name__ == "__main__":
    t1 = threading.Thread(target=main, name="t1")
    t2 = threading.Thread(target=main, name="t2")
    main()
