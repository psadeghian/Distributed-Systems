from xmlrpc.server import SimpleXMLRPCServer

def sort_array(arr):
    arr.sort()
    return arr

server = SimpleXMLRPCServer(("0.0.0.0", 5001), allow_none=True)
print("Remote server one is listening on port 5001...")
server.register_function(sort_array, "sort_array")
server.serve_forever()
