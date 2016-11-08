import xmlrpc.client, sys, pymysql
class Client:

    def __init__(self):
        pass
        self.directory_dict = {}
        self.last_file_name = -1
        self.file_node_map = {}

        self.cd_list = []
        self.host_port_tuples_list = [('127.0.0.1',6001)]
        #,('127.0.0.1',6002),('127.0.0.1',6003)
        self.command_dict = {"mkdir": Client.mkdir, "rm": Client.rm, "cp": Client.cp, "put": Client.put, "get": Client.get, "ls": Client.ls, "help": Client.help, "cd": Client.cd, "": Client.empty}

    def implement_put(self, host, port, file_addr):
        try:
            proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
            with open(file_addr, "rb") as handle:
                file_name = handle.name
                binary_data = xmlrpc.client.Binary(handle.read())
                message = proxy.put_on_server(binary_data, file_name)
            return message
        except:
            error_message = sys.exc_info()[0]
            return error_message

    def implement_get(self, host, port, file_addr_on_server, file_addr_on_client):
        try:
            proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
            with open(file_addr_on_client, "wb") as handle:
                handle.write(proxy.return_get(file_addr_on_server).data)
            message = "Done"
            return message
        except:
            error_message = sys.exc_info()[1]
            return error_message

    def implement_remove(self, host, port, path):
        try:
            proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
            message = proxy.remove(path)
            return message
        except:
            error_message = sys.exc_info()[1]
            return error_message

    def implement_copy(self, host, port, path):
        try:
            proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
            message = proxy.copy(path)
            return message
        except:
            error_message = sys.exc_info()[1]
            return error_message

    def mkdir(self, directory):
        directory = directory.strip()
        temp_dir_list = directory.split("/")
        temp_cd_dict = self.directory_dict
        if self.cd_list != []:
            for dir in self.cd_list:
                temp_cd_dict = temp_cd_dict[dir]
        for dir in temp_dir_list:
            if temp_cd_dict.get(dir) == None:
                temp_cd_dict[dir] = {}
                temp_cd_dict = temp_cd_dict[dir]
            else:
                temp_cd_dict = temp_cd_dict[dir]

    def rm(self, path):
        print("Removed: " + path)

    def cp(self, local_path, distributed_path):
        print("Copied from " + local_path + " to " + distributed_path)

    def put(self, local_):
        pass

    def get(self):
        pass

    def ls(self):
        temp_dir_dict = self.directory_dict
        for dir in self.cd_list:
            temp_dir_dict = temp_dir_dict[dir]
        for key in temp_dir_dict.keys():
            print(key)

        print(self.directory_dict)

    def help(self):
        for key in self.command_dict:
            print(key)
        # needs more work, perhaps read a help.text file to the user

    def cd(self, directory=None):
        temp_cd_dict = self.directory_dict
        temp_cd_list = self.cd_list.copy()
        if directory == None:
            temp_cd_list.clear()
        else:
            directory = directory.strip()
            if directory == "..":
                try:
                    temp_cd_list(-1)
                except:
                    pass
            else:
                temp_dir_list = directory.split("/")
                for dir in temp_dir_list:
                    temp_cd_list.append(dir)
        for dir in temp_cd_list:
            temp_cd_dict = temp_cd_dict[dir]
        self.cd_list = temp_cd_list

    def empty(self):
        pass

def main():
    machine = Client()
    while 1 == 1:
        if machine.cd_list == []:
            current_directory_string = "home $"
        else:
            current_directory_string = machine.cd_list[-1] + " $"

        try:
            command = input("Type your command:" + current_directory_string).strip()
            if command == "exit":
                break
            command_list = command.split(" ")
            foo = machine.command_dict[command_list.pop(0)]
            command_list.insert(0,machine)
            foo(*command_list)
        except:
            error_message = sys.exc_info()
            print(error_message)

        proxy = xmlrpc.client.ServerProxy("http://" + '127.0.0.1' + ":" + str(6001) + "/")
        message = proxy.size()
        print(message)

if __name__ == "__main__": main()

