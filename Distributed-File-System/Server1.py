from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client, sys, os

HOST = "0.0.0.0"
PORT = 6001
server = SimpleXMLRPCServer((HOST, PORT))
server.register_introspection_functions()

class RemoteFunctions:
    def put_on_server(self, encoded):
        with open("temp/temp.txt", "wb") as handle:
            handle.write(encoded.data)
            return "Done"

    def return_get(self, file_addr):
        with open(file_addr, "rb") as handle:
            return xmlrpc.client.Binary(handle.read())

    def remove(self,path):
        os.remove(path)
        return "File removed"
server.register_instance(RemoteFunctions())
print("Server 1 is listening at " + str(server.server_address[0])+ ":" + str(server.server_address[1]))
server.serve_forever()
