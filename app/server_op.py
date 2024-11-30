from app.RedisParser import RedisProtocolParser
import app.CommandHelper as cmd_helper
from app.common_file import CommonTools


def handle_commands_server(connection, server_arg, address=None):
    parser = RedisProtocolParser(server=server_arg)
    
    with connection:
        while True:
            data_stream = connection.recv(1024)
            
            if not data_stream:
                break
                
            commands = parser.feed(data_stream)
            print(commands, "dhruv commands")
            
            if commands[0] == "ping" or commands[0] == "PING":
                cmd_helper.send_pong(connection)
            elif commands[0][0].lower() == "echo":
                cmd_helper.echo(connection, commands)
            elif commands[0][0] == "set" or commands[0][0] == "SET" :
                # if CommonTools.replica_server:
                #     print("entered replica set if")
                #     cmd_helper.replica_set(connection, commands)
                # else:
                cmd_helper.set(connection, commands)
            elif commands[0][0].lower() == "get":
                cmd_helper.get(connection, commands)
            elif commands[0][0] == "INFO":
                cmd_helper.info(connection)
            elif commands[0][0] == "REPLCONF":   
                if CommonTools.replica_server:
                    cmd_helper.send_replconf_ack(connection, commands)
                else:
                    cmd_helper.master_receive_replconf(connection, address, commands)
            elif commands[0][0] == "PSYNC":
                cmd_helper.master_receive_psync(connection, address)
            # print(len(replica_backlog), "dhruv length backlog new 1")

        connection.close()
        
        
        
                
    
    

