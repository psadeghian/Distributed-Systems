from xmlrpc.server import SimpleXMLRPCServer

# Create server
HOST = "0.0.0.0"
# 10.0.2.2
PORT = 5001
server = SimpleXMLRPCServer((HOST, PORT))
#server.register_introspection_functions()

# Register an instance; all the methods of the instance are
# published as XML-RPC methods (in this case, just 'sort').
class RemoteFunctions:
    def sort(self, lyst):
        lyst.sort()
        return lyst

server.register_instance(RemoteFunctions())

# Run the server's main loop
print('Remote server 1 is listening at: ' + str(server.server_address[0]) + ":" + str(server.server_address[1]))
server.serve_forever()

