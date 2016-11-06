from xmlrpc.server import SimpleXMLRPCServer

HOST = "0.0.0.0"
PORT = 6003
ID = 3
server = SimpleXMLRPCServer((HOST, PORT))
server.register_introspection_functions()

class RemoteFunctions:
    def put_on_server(self, encoded):
        with open("temp/temp.txt", "wb") as handle:
            handle.write(encoded.data)
            return True

server.register_instance(RemoteFunctions())
print("Server 3 is listening at " + str(server.server_address[0])+ ":" + str(server.server_address[1]))
server.serve_forever()
