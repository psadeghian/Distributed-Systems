import xmlrpc.client

def implement_put(host, port, file_addr):
    with xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/") as proxy:
        with open(file_addr, "rb") as handle:
            binary_data = xmlrpc.client.Binary(handle.read())
            message = proxy.put_on_server(binary_data)
        return message

host_port_tuples_list = [('127.0.0.1',6001)]
#,('127.0.0.1',6002),('127.0.0.1',6003)

print(implement_put(host_port_tuples_list[0][0],host_port_tuples_list[0][1],"small_numbers.txt"))




