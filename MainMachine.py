import heapq, threading, time, socket
class MainMachine:
    """Class for the main server. This server distributes the workload to different machines. """

    def __init__(self):
        self.list_of_sorted_lists = []

    def __chunkify(self, lyst, n):
        if n > len(lyst):
            lenLyst = len(lyst)
            chunedLyst = [lyst[i::lenLyst] for i in range(lenLyst)]
        else:
            chunedLyst = [lyst[i::n] for i in range(n)]
        return chunedLyst

    def __conn_sen_rec(self, host, port, lyst):
        #print(host,port)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn = s.connect((host, port))
        #print(lyst[:10])
        pickledList = ','.join(str(e) for e in lyst)
        #print(pickledList)
        s.sendall(str.encode(pickledList))
        data = s.recv(8192)
        lyst = data.decode('utf-8').strip().split(",")
        lyst = [int(i) for i in lyst]
        self.list_of_sorted_lists.append(lyst)
        #print(lyst)
        #s.close()
        return lyst

    def merge_sort_by_threading(self, lyst, listOfHostPortTuples):
        threadList = []
        mergedList = []
        numOfThreads = len(listOfHostPortTuples)
        chunkedList = self.__chunkify(lyst, numOfThreads)
        conn = []
        for i in range(numOfThreads):
            t = threading.Thread(target=self.__conn_sen_rec, args=(listOfHostPortTuples[i][0], listOfHostPortTuples[i][1], chunkedList[i]))
            threadList.append(t)
            t.start()
        for t in threadList:
            t.join()
        for item in heapq.merge(*self.list_of_sorted_lists):
            mergedList.append(item)
        return mergedList

if __name__ == '__main__':
    listOfHostPortTuples = [('',5001),('',5002),('',5003)]

    numList = []
    f = open('/Users/Pasha/CSCI 604/Threading/Distributed-Systems/small_numbers.txt', 'r')
    for line in f.readlines():
        numList.append(int(line))
    f.close()

    print("First 30 elements of the unsorted list to be sorted:")
    print(numList[:30])
    print("",end='\n')
    print("Last 30 elements of the unsorted list to be sorted:")
    print(numList[-30:])
    print("",end='\n')

    start = time.time()
    machine = MainMachine()
    sorted_list = machine.merge_sort_by_threading(numList, listOfHostPortTuples)
    end = time.time()
    print("Merge Sort By " + str(len(listOfHostPortTuples)) + " machines: " + str((end - start)) + " seconds.",end='\n')
    print("Sorted " + str(len(sorted_list)) + " items.",end='\n')
    print("First 30 elements of the sorted list:",end='\n')
    print(sorted_list[:30],end='\n')
    print("",end='\n')
    print("Last 30 elements of the sorted list:",end='\n')
    print(sorted_list[-30:],end='\n')