import xmlrpc.client, sys, pymysql, os


class Client:

    def __init__(self):
        self.server_host = 'localhost'
        self.server_user = 'root'
        self.server_pass = ''
        self.server_db = 'dfs'
        # --------------------------------------------------------------------------------------------------------------
        self.machine_dict = {1: ('127.0.0.1', 6001), 2: ('127.0.0.1', 6002), 3: ('127.0.0.1', 6003)}
        # --------------------------------------------------------------------------------------------------------------
        self.__level = 0
        self.__parent = 'home'
        self.__current_dir = 'home'
        self.__command_dict = {"mkdir": Client.__mkdir, "rm": Client.__rm, "cp": Client.__cp, "put": Client.__put,
                             "get": Client.__get, "ls": Client.__ls, "help": Client.__help, "-format": Client.__format,
                             "cd": Client.__cd, "": Client.empty}
        self.__current_dir_address = "home/"



    def __format(self):

        print("Your client and servers have been formatted")

    def get_current_dir(self):
        return self.__current_dir

    def __mkdir(self, directory):
        directory = directory.strip()
        temp_dir_list = directory.split("/")

        # connect to the database
        connection = pymysql.connect(host=self.server_host,
                                     user=self.server_user,
                                     password=self.server_pass,
                                     db=self.server_db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        parent = self.__parent
        level = self.__level
        dir_name = self.__current_dir
        parent = dir_name
        address = self.__current_dir_address
        for i in range(len(temp_dir_list)):
            try:
                with connection.cursor() as cursor:
                    # create a new record
                    sql = "INSERT INTO `dir_map` (`level`, `dir_name`, `parent`, `address`) VALUES (%s, %s, %s, %s)"
                    cursor.execute(sql, (str(level + 1 + i), temp_dir_list[i], parent, address))
                # connection is not autocommit by default. So you must commit to save
                # your changes.
                connection.commit()
                parent = temp_dir_list[i]
                address = address + parent + "/"
            except pymysql.err.IntegrityError as e:
                # if the row exists in our table
                if e.args[0] == 1062:
                    parent = temp_dir_list[i]
                    address = address + parent + "/"
                else:
                    connection.close()
                    raise
        connection.close()

    def __rm(self, path_or_r, path_or_none=None):
        original_level = self.__level
        original_parent = self.__parent
        original_dir_name = self.__current_dir
        original_dir_name = self.__current_dir_address

        if path_or_r is None:
            pass

        elif path_or_r == "-r" and path_or_none is not None:
            self.__cd(path_or_none)

            parent_address_list = self.__current_dir_address.split("/")
            parent_address_list.pop(-2)
            parent_address = "/".join(parent_address_list)
            len_current_dir_address = len(self.__current_dir_address)

            connection = pymysql.connect(host=self.server_host,
                                         user=self.server_user,
                                         password=self.server_pass,
                                         db=self.server_db,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)
            try:

                with connection.cursor() as cursor:
                    sql = "DELETE FROM `dir_map` WHERE `dir_name` = %s AND `address` = %s"
                    cursor.execute(sql, (self.__current_dir, parent_address))
                connection.commit()

                with connection.cursor() as cursor:
                    sql = "DELETE FROM `dir_map` WHERE `level` > %s AND LEFT(`address`, %s) = %s"
                    cursor.execute(sql, (self.__level, len_current_dir_address, self.__current_dir_address))
                connection.commit()

                with connection.cursor() as cursor:
                    sql = "SELECT `file_id`, `machine_id` FROM `file_map` WHERE `level` > %s AND " \
                          "LEFT(`address`, %s) = %s"
                    cursor.execute(sql, (self.__level, len_current_dir_address, self.__current_dir_address))
                    row = cursor.fetchone()
                    while row is not None:
                        host = self.machine_dict[row['machine_id']][0]
                        port = self.machine_dict[row['machine_id']][1]
                        file_id = row['file_id']
                        proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
                        message = proxy.remove(file_id)
                        row = cursor.fetchone()

                with connection.cursor() as cursor:
                    sql = "DELETE FROM `file_map` WHERE `level` > %s AND " \
                          "LEFT(`address`, %s) = %s"
                    cursor.execute(sql, (self.__level, len_current_dir_address, self.__current_dir_address))
                connection.commit()

            finally:
                connection.close()
        else:
            path = path_or_r
            path_list = path.split("/")
            file_name = path_list.pop(-1)
            path = "/".join(path_list)
            if path != "":
                self.__cd(path)

            connection = pymysql.connect(host=self.server_host,
                                         user=self.server_user,
                                         password=self.server_pass,
                                         db=self.server_db,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)
            try:
                with connection.cursor() as cursor:
                    sql = "SELECT `file_id`, `machine_id` FROM `file_map` WHERE `file_name` = %s AND " \
                          "`address` = %s"
                    cursor.execute(sql, (file_name, self.__current_dir_address))
                    row = cursor.fetchone()

                    if row is not None:
                        host = self.machine_dict[row['machine_id']][0]
                        port = self.machine_dict[row['machine_id']][1]
                        file_id = row['file_id']
                        proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
                        message = proxy.remove(file_id)

                        sql = "DELETE FROM `file_map` WHERE `file_id` = %s"
                        cursor.execute(sql, (file_id,))
                        connection.commit()

            finally:
                connection.close()

        self.__level = original_level
        self.__parent = original_parent
        self.__current_dir = original_dir_name
        self.__current_dir_address = original_dir_name

        print("Done")

    def __cp(self, local_path, distributed_path):
        print("This function has not been implemented")

    def __put(self, local_path, distributed_path):
        original_level = self.__level
        original_parent = self.__parent
        original_dir_name = self.__current_dir
        original_dir_address = self.__current_dir_address

        # choose a machine id
        # and choose a file id for the file

        # connect to the database
        connection = pymysql.connect(host=self.server_host,
                                     user=self.server_user,
                                     password=self.server_pass,
                                     db=self.server_db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                sql = "SELECT MIN(`sum_file_size`) AS `min_file_size` FROM (SELECT SUM(`file_size`) AS " \
                      "`sum_file_size` FROM `file_map` GROUP BY `machine_id`) `T2`"
                cursor.execute(sql, ())
                result = cursor.fetchone()
                min_file_size = result['min_file_size']

                sql = "SELECT `machine_id` FROM (SELECT `machine_id`, SUM(`file_size`) AS `sum_file_size` " \
                      "FROM `file_map` GROUP BY `machine_id`) T2 WHERE T2.sum_file_size = %s"
                cursor.execute(sql, (min_file_size,))
                result = cursor.fetchone()

                machine_id = result['machine_id']

                sql = "SELECT MAX(`file_id`) AS `file_id` FROM `file_map`"
                cursor.execute(sql, ())
                result = cursor.fetchone()
                file_id = result['file_id'] + 1

        finally:
            connection.close()

        distributed_path = distributed_path.strip()
        distributed_path_list = distributed_path.split("/")
        file_name = distributed_path_list.pop(-1)
        distributed_path = "/".join(distributed_path_list)
        if distributed_path != "":
            self.__cd(distributed_path)

        file_size = os.path.getsize(local_path)
        host = self.machine_dict[machine_id][0]
        port = self.machine_dict[machine_id][1]
        proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
        with open(local_path, "rb") as handle:
            binary_data = xmlrpc.client.Binary(handle.read())

            connection = pymysql.connect(host=self.server_host,
                                         user=self.server_user,
                                         password=self.server_pass,
                                         db=self.server_db,
                                         charset='utf8mb4',
                                         cursorclass=pymysql.cursors.DictCursor)

            try:
                with connection.cursor() as cursor:
                    # create a new record
                    sql = "INSERT INTO file_map (`file_id`, `file_name`, `address`, `machine_id`, `file_size`," \
                          " `parent`, `level`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(sql, (file_id, file_name, self.__current_dir_address, machine_id, file_size,
                                         self.__current_dir, self.__level + 1))
                    # connection is not autocommit by default. So you must commit to save
                    # your changes.
                connection.commit()
            finally:
                connection.close()

            message = proxy.put(binary_data, file_id, file_name, file_size)

        self.__level = original_level
        self.__parent = original_parent
        self.__current_dir = original_dir_name
        self.__current_dir_address = original_dir_address
        print(message)

    def __get(self, distributed_path, local_path):
        original_level = self.__level
        original_parent = self.__parent
        original_dir_name = self.__current_dir
        original_dir_address = self.__current_dir_address

        distributed_path = distributed_path.strip()
        distributed_path_list = distributed_path.split("/")
        file_name = distributed_path_list.pop(-1)
        distributed_path = "/".join(distributed_path_list)

        if distributed_path != "":
            self.__cd(distributed_path)

        connection = pymysql.connect(host=self.server_host,
                                     user=self.server_user,
                                     password=self.server_pass,
                                     db=self.server_db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                sql = "SELECT `file_id`, `machine_id` FROM `file_map` WHERE `file_name` = %s AND `address` = %s"
                cursor.execute(sql, (file_name, self.__current_dir_address))
                result = cursor.fetchone()
                file_id = result["file_id"]
                machine_id = result["machine_id"]
        finally:
            connection.close()

        host = self.machine_dict[machine_id][0]
        port = self.machine_dict[machine_id][1]
        proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
        with open(local_path, "wb") as handle:
            handle.write(proxy.get(file_id).data)

        self.__level = original_level
        self.__parent = original_parent
        self.__current_dir = original_dir_name
        self.__current_dir_address = original_dir_address
        print("Done")

    def __ls(self):
        content = []
        connection = pymysql.connect(host=self.server_host,
                                     user=self.server_user,
                                     password=self.server_pass,
                                     db=self.server_db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                sql = "SELECT `dir_name` FROM `dir_map` WHERE `address` = %s"
                cursor.execute(sql, (self.__current_dir_address,))
                row = cursor.fetchone()
                while row is not None:
                    content.append(row['dir_name'])
                    row = cursor.fetchone()

                sql = "SELECT `file_name` FROM `file_map` WHERE `address` = %s"
                cursor.execute(sql, (self.__current_dir_address,))
                row = cursor.fetchone()
                while row is not None:
                    content.append(row['file_name'])
                    row = cursor.fetchone()
        finally:
            connection.close()

        for item in content:
            print(item)

    def __help(self):
        for key in self.__command_dict:
            print(key)
        # needs more work, perhaps read a help.text file to the user

    def __cd(self, directory=None):
        if directory == None:
            self.__level = 0
            self.__parent = 'home'
            self.__current_dir = 'home'
            self.__current_dir_address = 'home/'
        elif directory.strip()[0] == "/":
            directory = directory.strip()
            directory = directory[1:]
            self.__cd()
            if directory == "":
                directory = None
            self.__cd(directory)
        else:
            directory = directory.strip()
            if directory == "..":
                if self.__level <= 1:
                    if self.__level == 1:
                        self.__level = 0
                        self.__parent = 'home'
                        self.__current_dir = 'home'
                        self.__current_dir_address = 'home/'
                else:
                    # connect to the database
                    connection = pymysql.connect(host=self.server_host,
                                                 user=self.server_user,
                                                 password=self.server_pass,
                                                 db=self.server_db,
                                                 charset='utf8mb4',
                                                 cursorclass=pymysql.cursors.DictCursor)
                    level = self.__level
                    parent = self.__parent

                    dir_name = parent
                    level = level - 1
                    current_dir_address_list = self.__current_dir_address.split("/")
                    current_dir_address_list.pop(-2)
                    current_dir_address_list.pop(-2)
                    address = "/".join(current_dir_address_list)
                    # note we have to make sure that the directory is actually correct
                    try:
                        with connection.cursor() as cursor:
                            # create a new record
                            sql = "SELECT `parent` FROM `dir_map` WHERE `level` = %s AND `dir_name` = %s" \
                                  " AND `address` = %s"
                            cursor.execute(sql, (str(level), dir_name, address))
                            result = cursor.fetchone()
                            parent = result['parent']

                        connection.close()
                        self.__level = level
                        self.__parent = parent
                        self.__current_dir = dir_name
                        self.__current_dir_address = address + dir_name + "/"

                    except:
                        raise
            else:
                original_directory = directory
                directory = directory.strip()
                temp_dir_list = directory.split("/")

                # connect to the database
                connection = pymysql.connect(host=self.server_host,
                                             user=self.server_user,
                                             password=self.server_pass,
                                             db=self.server_db,
                                             charset='utf8mb4',
                                             cursorclass=pymysql.cursors.DictCursor)
                level = self.__level
                parent = self.__parent
                dir_name = self.__current_dir
                address = self.__current_dir_address
                # note we have to make sure that the directory is actually correct
                try:
                    for i in range(len(temp_dir_list)):

                        with connection.cursor() as cursor:
                            # create a new record
                            sql = "SELECT `level`, `parent`, `dir_name` FROM (SELECT `level`, `parent`, `dir_name`" \
                                  " FROM `dir_map` WHERE `address` = %s) `T2` WHERE `dir_name` = %s"
                            cursor.execute(sql, (address, temp_dir_list[i]))
                            result = cursor.fetchone()
                            level, parent, dir_name = result['level'], result['parent'], result['dir_name']
                            address = address + temp_dir_list[i] + "/"
                    connection.close()
                    self.__level = level
                    self.__parent = parent
                    self.__current_dir = dir_name
                    self.__current_dir_address = address

                except:
                    raise

    def empty(self):
        pass

    def command_handler(self, command):
        command_list = command.split(" ")
        foo = self.__command_dict[command_list.pop(0)]
        command_list.insert(0, self)
        foo(*command_list)


def main():
    machine = Client()
    while 1 == 1:
        current_directory_string = machine.get_current_dir() + " $"
    # try:
        command = input("Type your command:" + current_directory_string).strip()
        if command == "exit":
            break
        machine.command_handler(command)
    # except:
    #     error_message = sys.exc_info()
    #     print(error_message)

if __name__ == "__main__": main()

