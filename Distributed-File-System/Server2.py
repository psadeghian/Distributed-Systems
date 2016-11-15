from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client, sys, os, pymysql

HOST = "0.0.0.0"
PORT = 6002
ID = 2
server = SimpleXMLRPCServer((HOST, PORT))
server.register_introspection_functions()


class RemoteFunctions:

    def __init__(self):
        self.id = ID

    def get_id(self):
        return self.id

    def put(self, binary_data, file_id, file_name, file_size):
        # connect to the databse
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='dfs',
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

    def get(self, file_id):
        # connect to the databse
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='dfs',
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

    def remove(self, file_id):
        connection = pymysql.connect(host='localhost',
                                     user='root',
                                     password='',
                                     db='dfs',
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
