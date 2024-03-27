# Uncomment this to pass the first stage
import socket
import threading


def handleRequest(connection):
    pong = "+PONG\r\n"
    client_data = b""
    
    with connection:
        while True:
            data_stream = connection.recv(1024)
            if not data_stream:
                break
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
