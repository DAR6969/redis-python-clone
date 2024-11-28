class RedisProtocolParser:
    def __init__(self):
        self.buffer = b''  # Buffer to hold incoming data
    
    def feed(self, data):
        self.buffer += data 
        commands = self.parse_buffer()
        print(commands, "new parser")
        return commands
    
    def parse_buffer(self):
        parsed_commands = []
        while True:
            if not self.buffer:
                break
            
            array_start = self.buffer.find(b'*')
            if array_start != -1:
                self.buffer = self.buffer[array_start:]
                print("enetred parsing code", self.buffer)
                # end_of_command = self.find_command_end()
                # print(end_of_command, "end of command")
                # if end_of_command is None:
                #     break
                
                # command_data = self.buffer[:end_of_command + 2]
                # print(command_data, "command data")
                # self.buffer = self.buffer[end_of_command + 2:]
                # print(self.buffer, "remaining buffer")
                # parsed_commands.append(self.parse(self.buffer))
                break
            else:
                break
    
        return self.parse(self.buffer)
    
    def find_command_end(self):
        try: 
            command_start = self.buffer.find(b'*')
            if command_start == '-1':
                print("* not found")
                return None
            
            print(command_start, "command start")
            num_args = int(self.buffer[command_start + 1: command_start+2].decode())
            expected_length = command_start + 2
            
            for _ in range(num_args):
                # Find the length of each argument
                arg_length_start = self.buffer.find(b'$') + 1
                arg_length_end = self.buffer.find(b'\r\n', arg_length_start)
                if arg_length_end == -1:
                    return None  # Not enough data

                arg_length = int(self.buffer[arg_length_start:arg_length_end].decode())
                expected_length = arg_length_end + 2 + arg_length + 2  # Move past the argument and its CRLF

            return expected_length if expected_length <= len(self.buffer) else None
        
        except Exception as e:
            print("eerror in find_command_end", e)
            return
            
    
    def parse_command(self, command_data):
        
        commands = command_data.decode().split('\r\n')[:-1]
        print(commands, "parse func commands")
        parsed_args = []
        num_args = int(commands[0][1:])
        for i in range(1, num_args +1):
            arg_len = int(commands[0][1:])
            arg = commands[i + num_args][:arg_len]
            print(arg, "loop arg")
            parsed_args.append(arg)
        return parsed_args
    
    
    @staticmethod
    def parse(data):
        commands = data.decode().split('\r\n')[:-1]
        print(commands, "dhruv function commands")
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
            bulk_string += f"${len(str(arg))}\r\n{arg}\r\n"
        return bulk_string.encode()
    
    def create_bulk_string_bytes(input_bytes):
        length = len(input_bytes)
        encoded_string = f"${length}\r\n".encode()
        encoded_string += input_bytes
        # encoded_string += "\r\n"
        return encoded_string
    
    def create_array(*args):
        num_elements = len(args)
        array_string = f"*{num_elements}\r\n"
        for arg in args:
            if isinstance(arg, int):
                arg = str(arg)
            array_string += f"${len(arg)}\r\n{arg}\r\n"
        return array_string.encode()