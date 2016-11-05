import socket

HOST = ''
PORT = 5002
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    s.bind((HOST, PORT))
except socket.error as e:
    print(str(e))

s.listen(1)
print('Waiting for connection')
def makeConnection(conn):

    while True:
        data = conn.recv(8192)
        #print(data)
        lyst = data.decode('utf-8').split(",")
        for i in range(len(lyst)):
            if str(lyst[i]).strip() == '':
                lyst.pop(i)
        lyst = [int(i) for i in lyst]
        #print(lyst)
        #print(type(lyst))
        lyst.sort()
        #print(lyst[:20])
        reply = ",".join(str(e) for e in lyst)
        if not data:
            break
        conn.sendall(str.encode(reply))

    conn.close()

while True:
    conn, addr = s.accept()
    print('Remote machine 2 connected to:'+addr[0]+':'+str(addr[1]))
    makeConnection(conn)