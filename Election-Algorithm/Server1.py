from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client, sys, time, threading

# Create server
HOST = "0.0.0.0"
# 10.0.2.2
PORT = 5000
NODE = 0
LEADER = 2
node_list = [0, 1, 2]
node_list_alive = [0, 1, 2]
machines = [('127.0.0.1',5000),('127.0.0.1',5001),('127.0.0.1',5002)]

def nominate_send_election_message(nominees):
    nominees.append(NODE)
    node_list_alive = nominees
    index = NODE
    for i in range(len(node_list)-1):
        if index + 1 > max(node_list):
            index = 0
        else:
            index = index + 1
        try:
            host = machines[index][0]
            port = machines[index][1]
            with xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/") as proxy:
                returned_value = proxy.receive_nominees(nominees)
            break
        except ConnectionRefusedError:
            continue

def send_leader_message(lyst):
    index = NODE
    for i in range(len(node_list)-1):
        if index + 1 > max(node_list):
            index = 0
        else:
            index = index + 1
        try:
            host = machines[index][0]
            port = machines[index][1]
            with xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/") as proxy:
                returned_value = proxy.receive_leader(lyst)
            break
        except ConnectionRefusedError:
            continue


def start_threading():
    thread_list = []
    t = threading.Thread(target=thread_function,args=("client",))
    thread_list.append(t)
    t.start()
    t = threading.Thread(target=thread_function,args=("server",))
    thread_list.append(t)
    t.start()

def run_server():

    # Register an instance; all the methods of the instance are
    # published as XML-RPC methods (in this case, just 'sort').
    class RemoteFunctions:
        def sort(self, lyst):
            lyst.sort()
            return lyst
        def get_node(self):
            return NODE
        def receive_nominees(self, nominees):
            try:
                nominees.index(NODE)
                LEADER = max(nominees)
                leader_message = [LEADER, NODE]
                send_leader_message(leader_message)
                print("Leader message initiated. Below is the leader message sent: ")
                print(leader_message)
            except ValueError:
                nominate_send_election_message(nominees)
            return "Thank You!"
        def receive_leader(self, leader_message):
            if leader_message[1] == NODE:
                print("Leader message has been passed around successfully.")
                print("New leader is " + str(LEADER))
                pass
            else:
                LEADER = leader_message[0]
                send_leader_message(leader_message)
            return "Thank You!"

    server = SimpleXMLRPCServer((HOST, PORT))
    #server.register_introspection_functions()
    server.register_instance(RemoteFunctions())
    # Run the server's main loop
    print('Node '+ str(NODE) + ' is listening at: ' + str(server.server_address[0]) + ":" + str(server.server_address[1]))
    server.serve_forever()

def run_client():
    delay(30)
    client_main_loop(10)

def delay(seconds):
    start_time = time.time()
    dif_time = 0
    while 1 == 1:
        dif_time = time.time() - start_time
        if dif_time > seconds:
            break

def client_main_loop(checking_interval):
    start_time = time.time()
    dif_time = 0
    while 1 == 1:
        dif_time = time.time() - start_time

        if dif_time >= checking_interval:
            index = NODE
            for i in range(len(node_list)-1):
                if index + 1 > max(node_list):
                    index = 0
                else:
                    index = index + 1
                try:
                    node_list_alive.index(index)
                    try:
                        host = machines[index][0]
                        port = machines[index][1]
                        with xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/") as proxy:
                            returned_node = proxy.get_node()
                        print("everything cool!")
                    except ConnectionRefusedError:
                        print(sys.exc_info()[0])
                        lyst = []
                        nominate_send_election_message(lyst)
                        print("Node "+str(index)+" is dead. Election Started by Node "+str(NODE))

                except ValueError:
                    pass
            start_time = time.time()
            print(start_time)
            dif_time = 0

def thread_function(the_string):
    if the_string == "client":
        run_client()
    elif the_string == "server":
        run_server()

start_threading()