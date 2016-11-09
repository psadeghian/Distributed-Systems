from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client, sys, os, pymysql

HOST = "0.0.0.0"
PORT = 6003
ID = 3
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
                sql = "INSERT INTO machine_3 (file_id, file_name, file, file_size) VALUES (%s, %s, %s, %s)"
                binary_data = binary_data.data
                cursor.execute(sql, (file_id, file_name, binary_data, file_size))
            # connection is not autocommit by default. So you must commit to save
            # your changes.
            connection.commit()
        finally:
            connection.close()

        return "Done"

    # def return_get(self, file_addr):
    #     with open(file_addr, "rb") as handle:
    #         return xmlrpc.client.Binary(handle.read())

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
                sql = "SELECT `file` FROM `machine_3` WHERE `file_id` = %s"
                cursor.execute(sql, (file_id,))
                result = cursor.fetchone()
                file = result["file"]
        finally:
            connection.close()

        return file

    def remove(self,path):
        return "Not working yet"

    def copy(self,path):
        return "Not working yet"


server.register_instance(RemoteFunctions())
print("Server" + str(ID) + " is listening at " + str(server.server_address[0])+ ":" + str(server.server_address[1]))
server.serve_forever()
