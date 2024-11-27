class RedisProtocolParser:
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