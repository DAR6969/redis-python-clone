from app.RedisParser import RedisProtocolParser
import app.CommandHelper as cmd_helper


def handle_commands_server(connection, address):
    parser = RedisProtocolParser(server=True)
    
    with connection:
        while True:
            data_stream = connection.recv(1024)
            
            if not data_stream:
                break
                
            commands = parser.feed(data_stream)
            print(commands, "dhruv commands")
            
            if commands[0] == "ping" or commands[0] == "PING":
                cmd_helper.send_pong(connection)
            elif commands[0][0] == "echo":
                cmd_helper.echo(connection, commands)
            elif commands[0][0] == "set" or commands[0][0] == "SET" :
                cmd_helper.set(connection, commands)
            elif commands[0][0].lower() == "get":
                cmd_helper.get(connection, commands)
            elif commands[0][0] == "INFO":
                cmd_helper.info()
            elif commands[0][0] == "REPLCONF":   
                cmd_helper.master_receive_replconf(connection, address, commands)
            elif commands[0][0] == "PSYNC":
                cmd_helper.master_receive_psync(connection, address)
            # print(len(replica_backlog), "dhruv length backlog new 1")

        connection.close()
        
        
        
                
    
    

