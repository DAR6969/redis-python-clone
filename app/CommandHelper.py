import base64
import threading
from app.common_file import CommonTools
from app.RedisParser import RedisProtocolParser

common = CommonTools()

def send_pong(connection):
    connection.send(common.pong)

def echo(connection, commands):
    response = RedisProtocolParser.feed(commands[0][1])
    connection.send(response)

def set(connection, commands):
    common.get_map[commands[0][1]] = commands[0][2]
    
    if(common.received_replica_handshake):
        rep_command = RedisProtocolParser.create_array(*commands[0])
        connection.send(common.ok.encode())
        
        if len(common.slaves) > 0:
            for address in common.slaves.keys():
                if(common.slaves[str(address)]["connected"]):
                    try:
                        print("propogating comm to slave")
                        print(common.slaves, "druv slave list")
                        # print(connection, "dhruv connection actual")
                        common.slaves[str(address)]["connection"].sendall(rep_command)
                    except Exception as e:
                        print("Error:", e)
                                    
        common.replica_backlog.append(rep_command)
        
    else:
        connection.send(common.ok.encode())
    if(len(commands[0])>3 and commands[0][3] == "px"):
        print("hello!")
        key_remove = commands[0][1]
        delay = int(commands[0][4])/1000
        timer = threading.Timer(delay, remove_key_px, args=(key_remove, delay))
        timer.start()
    print(common.get_map, "dhruv map set")

def replica_set(commands):
    for command in commands:
        common.get_map[command[0][1]] = command[0][2]
    
    print(common.get_map, "updated get_map")
 
def remove_key_px(key, delay):
    # time.sleep(delay)  # Sleep for the specified delay
    if key in common.get_map:
        del common.get_map[key]
    print(common.get_map, "dhruv map set 2")

def get(connection, commands):
    print(common.get_map, "dhruv map get")
    #propogate to slaves
    if(common.received_replica_handshake):
        rep_command = RedisProtocolParser.create_array(*commands[0])
        connection.send(common.ok.encode())
        # connection.send(rep_command)
        # global replica_backlog
        if len(common.slaves) > 0:
            for address in common.slaves.keys():
                if(common.slaves[str(address)]["connected"]):
                    try:
                        print("propogating comm to slave")
                        print(common.slaves, "druv slave list")
                        print(connection, "dhruv connection actual")
                        common.slaves[str(address)]["connection"].sendall(rep_command)
                    except Exception as e:
                        print("Error:", e)
    if commands[0][1] in common.get_map:
        response = RedisProtocolParser.encode_redis_bulk_string(common.get_map[commands[0][1]])
    else:
        response = common.null_bulk.encode()
    connection.send(response) 

def info():
    if common.replica_server:
        print("replicate comm recieved")
        common.connection.send("$10\r\nrole:slave\r\n".encode())    
    else:
        print("master comm received")
        role=f"role:master"
        master_replid = f"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        master_repl_offset = f"master_repl_offset:0"
        new_string = role+master_replid+master_repl_offset
        # role_info_result = RedisProtocolParser.create_bulk_string("role:master")
        new_info_result = RedisProtocolParser.create_bulk_string(new_string)
        print(new_info_result, "dhruv new result")
        common.connection.send(new_info_result)
        
def master_receive_replconf(connection, address, commands):
    ok_response = "+OK\r\n"
    if(commands[0][1] == "listening-port"):
        replica_port = commands[0][2]
        common.slaves[str(address)] = {
            "port": replica_port,
            "connection": connection,
            "connected": False,
        }
    connection.send(ok_response.encode())
    
def master_receive_psync(connection, address):
    if not common.replica_server:
        master_replid = f"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        sync_res = "+FULLRESYNC " + master_replid + " 0\r\n"
        connection.send(sync_res.encode())
        # print(address, "address")
        common.slaves[str(address)]["connected"] = True
        
        empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
        # length_rdb = len(empty_rdb_base64)
        empty_rdb_bytes = base64.b64decode(empty_rdb_base64)
        empty_rdb_binary = RedisProtocolParser.create_bulk_string_bytes(empty_rdb_bytes)
        print(empty_rdb_binary, "Dhruv binary 3 new")
        connection.send(empty_rdb_binary)
        # global received_replica_handshake
        common.received_replica_handshake = True
    
    