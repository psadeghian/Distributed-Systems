from xmlrpc.server import SimpleXMLRPCServer
import  pymysql, warnings

# ----------------------------------------------------------------------------------------------------------------------
HOST = "0.0.0.0"
PORT = 6003
ID = 3
# ----------------------------------------------------------------------------------------------------------------------
server = SimpleXMLRPCServer((HOST, PORT))
server.register_introspection_functions()


class RemoteFunctions:

    def __init__(self):
        self.__id = ID
        self.__server_host = HOST
        self.__server_user = 'root'
        self.__server_pass = ''

    def get_id(self):
        return self.__id

    def format(self, server_db, locally):
        if locally != "locally":
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                connection = pymysql.connect(host=self.__server_host, user=self.__server_user, password=self.__server_pass)
                with connection.cursor() as cursor:
                    sql = "DROP DATABASE IF EXISTS "
                    sql = sql + server_db
                    cursor.execute(sql)

                    sql = "CREATE DATABASE "
                    sql = sql + server_db
                    cursor.execute(sql)
                connection.commit()

        connection = pymysql.connect(host=self.__server_host,
                                     user=self.__server_user,
                                     password=self.__server_pass,
                                     db=server_db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            sql = "CREATE TABLE machine_%s (`file_id` INT NOT NULL PRIMARY KEY," \
                  " `file_name` VARCHAR(30) NOT NULL, `file` BLOB, `file_size` INT NOT NULL)"
            cursor.execute(sql, (ID, ))

        connection.commit()
        return "Done"

    def put(self, binary_data, file_id, file_name, file_size, server_db):
        # connect to the databse
        connection = pymysql.connect(host=self.__server_host,
                                     user=self.__server_user,
                                     password=self.__server_pass,
                                     db=server_db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # create a new record
                sql = "INSERT INTO machine_%s (file_id, file_name, file, file_size) VALUES (%s, %s, %s, %s)"
                binary_data = binary_data.data
                cursor.execute(sql, (ID, file_id, file_name, binary_data, file_size))
            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()

        return "Done"

    def get(self, file_id, server_db):
        # connect to the databse
        connection = pymysql.connect(host=self.__server_host,
                                     user=self.__server_user,
                                     password=self.__server_pass,
                                     db=server_db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # create a new record
                sql = "SELECT `file` FROM `machine_%s` WHERE `file_id` = %s"
                cursor.execute(sql, (ID, file_id))
                result = cursor.fetchone()
                file = result["file"]
        finally:
            connection.close()

        return file

    def remove(self, file_id, server_db):
        connection = pymysql.connect(host=self.__server_host,
                                     user=self.__server_user,
                                     password=self.__server_pass,
                                     db=server_db,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # create a new record
                sql = "DELETE FROM `machine_%s` WHERE `file_id` = %s"
                cursor.execute(sql, (ID, file_id))
            connection.commit()
        finally:
            connection.close()
        return "Done"

    def copy(self,path):
        return "Not working yet"


server.register_instance(RemoteFunctions())
print("Server" + str(ID) + " is listening at " + str(server.server_address[0])+ ":" + str(server.server_address[1]))
server.serve_forever()
