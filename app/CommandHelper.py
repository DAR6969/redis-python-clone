import base64
import threading
from app.common_file import CommonTools
from app.RedisParser import RedisProtocolParser

common = CommonTools()

def send_pong(connection):
    connection.send(CommonTools.pong.encode())

def echo(connection, commands):
    response = RedisProtocolParser.encode_redis_bulk_string(commands[0][1])
    connection.send(response)

def set(connection, commands):
    CommonTools.get_map[commands[0][1]] = commands[0][2]
    
    if(CommonTools.received_replica_handshake):
        rep_command = RedisProtocolParser.create_array(*commands[0])
        connection.send(CommonTools.ok.encode())
        
        if len(CommonTools.slaves) > 0:
            for address in CommonTools.slaves.keys():
                if(CommonTools.slaves[str(address)]["connected"]):
                    try:
                        print("propogating comm to slave")
                        print(CommonTools.slaves, "druv slave list")
                        # print(connection, "dhruv connection actual")
                        CommonTools.slaves[str(address)]["connection"].sendall(rep_command)
                    except Exception as e:
                        print("Error:", e)
                                    
        CommonTools.replica_backlog.append(rep_command)
        
    else:
        connection.send(CommonTools.ok.encode())
    if(len(commands[0])>3 and commands[0][3] == "px"):
        print("hello!")
        key_remove = commands[0][1]
        delay = int(commands[0][4])/1000
        timer = threading.Timer(delay, remove_key_px, args=(key_remove, delay))
        timer.start()
    print(CommonTools.get_map, "dhruv map set")

def replica_set(commands):
    for command in commands:
        CommonTools.get_map[command[1]] = command[2]
    
    print(CommonTools.get_map, "updated get_map")
 
def remove_key_px(key, delay):
    # time.sleep(delay)  # Sleep for the specified delay
    if key in CommonTools.get_map:
        del CommonTools.get_map[key]
    print(CommonTools.get_map, "dhruv map set 2")

def get(connection, commands):
    print(CommonTools.get_map, "dhruv map get")
    #propogate to slaves
    if(CommonTools.received_replica_handshake):
        rep_command = RedisProtocolParser.create_array(*commands[0])
        connection.send(CommonTools.ok.encode())
        # connection.send(rep_command)
        # global replica_backlog
        if len(CommonTools.slaves) > 0:
            for address in CommonTools.slaves.keys():
                if(CommonTools.slaves[str(address)]["connected"]):
                    try:
                        print("propogating comm to slave")
                        print(CommonTools.slaves, "druv slave list")
                        print(connection, "dhruv connection actual")
                        CommonTools.slaves[str(address)]["connection"].sendall(rep_command)
                    except Exception as e:
                        print("Error:", e)
    if commands[0][1] in CommonTools.get_map:
        response = RedisProtocolParser.encode_redis_bulk_string(CommonTools.get_map[commands[0][1]])
    else:
        response = CommonTools.null_bulk.encode()
    connection.send(response) 

def info(connection):
    if CommonTools.replica_server:
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
        
def master_receive_replconf(connection, address, commands):
    ok_response = "+OK\r\n"
    if(commands[0][1] == "listening-port"):
        replica_port = commands[0][2]
        CommonTools.slaves[str(address)] = {
            "port": replica_port,
            "connection": connection,
            "connected": False,
        }
    connection.send(ok_response.encode())
    
def master_receive_psync(connection, address):
    if not CommonTools.replica_server:
        master_replid = f"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        sync_res = "+FULLRESYNC " + master_replid + " 0\r\n"
        connection.send(sync_res.encode())
        # print(address, "address")
        CommonTools.slaves[str(address)]["connected"] = True
        
        empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
        # length_rdb = len(empty_rdb_base64)
        empty_rdb_bytes = base64.b64decode(empty_rdb_base64)
        empty_rdb_binary = RedisProtocolParser.create_bulk_string_bytes(empty_rdb_bytes)
        print(empty_rdb_binary, "Dhruv binary 3 new")
        connection.send(empty_rdb_binary)
        # global received_replica_handshake
        CommonTools.received_replica_handshake = True
    
def send_replconf_ack(connection):
    connection.send(RedisProtocolParser.create_array(*CommonTools.REPLCONF_ack.split()))
    