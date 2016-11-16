import heapq, threading, time, xmlrpc.client
class Client:
    """Class for the client. This client distributes the workload to different servers. """

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
        with xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/") as proxy:
            # "http://" +
            sorted_list = proxy.sort(lyst)
        self.list_of_sorted_lists.append(sorted_list)
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
    listOfHostPortTuples = [('127.0.0.1',5001),('127.0.0.1',5002),('127.0.0.1',5003)]
    num_list = []
    f = open('small_numbers.txt', 'r')
    for line in f.readlines():
        num_list.append(int(line))
    f.close()

    print("First 30 elements of the unsorted list to be sorted:")
    print(num_list[:30])
    print("",end='\n')
    print("Last 30 elements of the unsorted list to be sorted:")
    print(num_list[-30:])
    print("",end='\n')

    start = time.time()
    client = Client()
    sorted_list = client.merge_sort_by_threading(num_list, listOfHostPortTuples)
    end = time.time()
    print("Merge Sort By " + str(len(listOfHostPortTuples)) + " machines: " + str((end - start)) + " seconds.",end='\n')
    print("Sorted " + str(len(sorted_list)) + " items.",end='\n')
    print("First 30 elements of the sorted list:",end='\n')
    print(sorted_list[:30],end='\n')
    print("",end='\n')
    print("Last 30 elements of the sorted list:",end='\n')
    print(sorted_list[-30:],end='\n')