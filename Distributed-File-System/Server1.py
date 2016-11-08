from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client, sys, os

HOST = "0.0.0.0"
PORT = 6001
ID = 1
server = SimpleXMLRPCServer((HOST, PORT))
server.register_introspection_functions()

class RemoteFunctions:
    def __init__(self):
        self.id = ID

    def get_id(self):
        return self.id

    def put_on_server(self, encoded, file_name):
        path = "data_directory/" + file_name
        print(path)
        with open(path, "wb") as handle:
            handle.write(encoded.data)
            return "Done"

    def return_get(self, file_addr):
        with open(file_addr, "rb") as handle:
            return xmlrpc.client.Binary(handle.read())

    def remove(self,path):
        os.remove(path)
        return "File removed"

    def copy(self,path):
        return "Not working yet"

    def size(self):
        value = os.path.getsize("data_directory/small_numbers.txt")
        return value


server.register_instance(RemoteFunctions())
print("Server 1 is listening at " + str(server.server_address[0])+ ":" + str(server.server_address[1]))
server.serve_forever()
