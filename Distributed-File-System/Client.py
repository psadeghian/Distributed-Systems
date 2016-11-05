import xmlrpc.client, sys

def implement_put(host, port, file_addr):
    try:
        proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
        with open(file_addr, "rb") as handle:
            binary_data = xmlrpc.client.Binary(handle.read())
            message = proxy.put_on_server(binary_data)
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

host_port_tuples_list = [('127.0.0.1',6001)]
#,('127.0.0.1',6002),('127.0.0.1',6003)

print(implement_put(host_port_tuples_list[0][0],host_port_tuples_list[0][1],"small_numbers.txt"))
print(implement_get(host_port_tuples_list[0][0],host_port_tuples_list[0][1],"temp/temp.txt","test_numbers.txt"))
print(implement_remove(host_port_tuples_list[0][0],host_port_tuples_list[0][1],"temp/temp.txt"))


