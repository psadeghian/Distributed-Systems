import xmlrpc.client, sys

directory_list = []
directory_list.append("user/")

def implement_put(host, port, file_addr):
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

def implement_get(host, port, file_addr_on_server, file_addr_on_client):
    try:
        proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
        with open(file_addr_on_client, "wb") as handle:
            handle.write(proxy.return_get(file_addr_on_server).data)
        message = "Done"
        return message
    except:
        error_message = sys.exc_info()[1]
        return error_message

def implement_remove(host, port, path):
    try:
        proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
        message = proxy.remove(path)
        return message
    except:
        error_message = sys.exc_info()[1]
        return error_message

def implement_copy(host, port, path):
    try:
        proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
        message = proxy.copy(path)
        return message
    except:
        error_message = sys.exc_info()[1]
        return error_message

def mkdir(directory):
    print("Created directory: " + directory)

def rm(path):
    print("Removed: " + path)

def cp(local_path, distributed_path):
    print("Copied from " + local_path + " to " + distributed_path)

def put():
    pass

def get():
    pass

def ls():
    pass

def help():
    pass

def cd():
    pass

host_port_tuples_list = [('127.0.0.1',6001)]
#,('127.0.0.1',6002),('127.0.0.1',6003)

#print(implement_put(host_port_tuples_list[0][0],host_port_tuples_list[0][1],"small_numbers.txt"))
#print(implement_put(host_port_tuples_list[0][0],host_port_tuples_list[0][1],"small_numbers.txt"))
#print(implement_get(host_port_tuples_list[0][0],host_port_tuples_list[0][1],"temp/temp.txt","test_numbers.txt"))
#print(implement_remove(host_port_tuples_list[0][0],host_port_tuples_list[0][1],"temp/temp.txt"))

for i in range(1):
    command_dict = {"mkdir": mkdir, "rm": rm, "cp": cp, "put": put, "get": get, "ls": ls, "help": help, "cd": cd}
    command = input("Type your command: ").strip()
    command_list = command.split(" ")
    foo = command_dict[command_list.pop(0)]
    foo(*command_list)


