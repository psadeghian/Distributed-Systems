import xmlrpc.client, sys, pymysql
class Client:

    def __init__(self):
        self.level = 0
        self.parent = 'home'
        self.cd = 'home'



        self.directory_dict = {}

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

        # connect to the databse
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='dfs',
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        parent = self.parent
        level = self.level
        dir_name = self.cd
        for i in range(len(temp_dir_list)):
            try:
                with connection.cursor() as cursor:
                    # create a new record
                    sql = "INSERT INTO `dir_map` (`level`, `dir_name`, `parent`) VALUES (%s, %s, %s)"
                    cursor.execute(sql, (str(level + 1 + i), temp_dir_list[i], parent))
                # connection is not autocommit by default. So you must commit to save
                # your changes.
                connection.commit()
                parent = temp_dir_list[i]
            except pymysql.err.IntegrityError as e:
                if e.args[0] == 1062:
                    parent = temp_dir_list[i]
                else:
                    connection.close()
                    raise
        connection.close()

    def rm(self, path):
        print("Removed: " + path)

    def cp(self, local_path, distributed_path):
        print("Copied from " + local_path + " to " + distributed_path)

    def put(self, local_):
        pass

    def get(self):
        pass

    def ls(self):
        pass

    def help(self):
        for key in self.command_dict:
            print(key)
        # needs more work, perhaps read a help.text file to the user

    def cd(self, directory=None):
        if directory == None:
            self.level = 0
            self.parent = 'home'
            self.cd = 'home'
        else:
            directory = directory.strip()
            if directory == "..":
                if self.level <= 1:
                    if self.level == 1:
                        self.level = 0
                        self.parent = 'home'
                        self.cd = 'home'
                else:
                    # connect to the databse
                    connection = pymysql.connect(host='localhost',
                                                 user='root',
                                                 password='',
                                                 db='dfs',
                                                 charset='utf8mb4',
                                                 cursorclass=pymysql.cursors.DictCursor)
                    level = self.level
                    parent = self.parent
                    dir_name = self.cd

                    dir_name = parent
                    level = level - 1
                    # note we have to make sure that the directory is actually correct
                    try:
                        with connection.cursor() as cursor:
                            # create a new record
                            sql = "SELECT `parent` FROM `dir_map` WHERE `level` = %s AND `dir_name` = %s"
                            cursor.execute(sql, (str(level), dir_name))
                            result = cursor.fetchone()
                            parent = result['parent']

                        connection.close()
                        self.level = level
                        self.parent = parent
                        self.cd = dir_name

                    except:
                        raise
            else:
                directory = directory.strip()
                temp_dir_list = directory.split("/")

                # connect to the databse
                connection = pymysql.connect(host='localhost',
                                             user='root',
                                             password='',
                                             db='dfs',
                                             charset='utf8mb4',
                                             cursorclass=pymysql.cursors.DictCursor)
                level = self.level
                parent = self.parent
                dir_name = self.cd

                # note we have to make sure that the directory is actually correct
                try:
                    for i in range(len(temp_dir_list)):
                        with connection.cursor() as cursor:
                            # create a new record
                            sql = "SELECT `level`, `parent`, `dir_name` FROM `dir_map` WHERE `level` = %s AND `dir_name` = %s"
                            cursor.execute(sql, (str(level + 1), temp_dir_list[i]))
                            result = cursor.fetchone()
                            level, parent, dir_name = result['level'], result['parent'], result['dir_name']
                    connection.close()
                    self.level = level
                    self.parent = parent
                    self.cd = dir_name

                except:
                    raise
    def empty(self):
        pass

def main():
    machine = Client()
    while 1 == 1:
        current_directory_string = machine.cd + " $"
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

