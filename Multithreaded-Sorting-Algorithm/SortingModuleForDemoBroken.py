import heapq, threading, multiprocessing, time

class Sorter:
    """Class for sorting a list of numbers using threading or multi processing. """
    def __init__(self):
        self.list_of_sorted_lists = []

    def __chunkify(self, lyst,n):
        if n > len(lyst):
            lenLyst = len(lyst)
            chunedLyst = [lyst[i::lenLyst] for i in range(lenLyst)]
        else:
            chunedLyst = [lyst[i::n] for i in range(n)]
        return chunedLyst

    def __sortList(self, lyst):
        lyst.sort()
        self.list_of_sorted_lists.append(lyst)
        return lyst

    def mergeSortByThreading(self, lyst, numOfThreads):
        threadList = []
        mergedList = []
        chunkedList = self.__chunkify(lyst, numOfThreads)
        self.list_of_sorted_lists = []
        for i in range(numOfThreads):
            t = threading.Thread(target=self.__sortList, args=(chunkedList[i],))
            threadList.append(t)
            t.start()
        for t in threadList:
            t.join()
        for item in heapq.merge(*self.list_of_sorted_lists):
            mergedList.append(item)
        return mergedList

    def mergeSortByMultiProcessing(self, lyst, numOfProcesses):
        processList = []
        mergedList = []

        chunkedList = self.__chunkify(lyst, numOfProcesses)
        for i in range(numOfProcesses):
            p = multiprocessing.Process(target=self.__sortList, args=(chunkedList[i],))
            processList.append(p)
            p.start()
        for p in processList:
            p.join()
        for item in heapq.merge(*self.list_of_sorted_lists):
            mergedList.append(item)
        return mergedList

if __name__ == '__main__':
    numOfThreads = 3

    numList = []
    f = open ('numbers.txt', 'r')
    for line in f.readlines():
        numList.append(int(line))
    f.close()

    print("First 30 elements of the unsorted list to be sorted by Merge Sort By Threading:",end='\n')
    print(numList[:30],end='\n')
    print("",end='\n')
    print("Last 30 elements of the unsorted list to be sorted by Merge Sort By Threading:",end='\n')
    print(numList[-30:],end='\n')
    print("",end='\n')
    sorter = Sorter()
    start = start = time.time()
    sorted_list = sorter.mergeSortByThreading(numList, numOfThreads)
    end = end = time.time()
    print("Merge Sort By Threading: " + str((end - start)) + " seconds.")
    print("Sorted " + str(len(sorted_list)) + " items.")
    print("First 30 elements of the sorted list:")
    print(sorted_list[:30],end='\n')
    print("",end='\n')
    print("Last 30 elements of the sorted list:",end='\n')
    print(sorted_list[-30:],end='\n')
    print("",end='\n')

    print("---------------------------------------",end='\n')

    numList = []
    f = open ('numbers.txt', 'r')
    for line in f.readlines():
        numList.append(int(line))
    f.close()

    print("First 30 elements of the unsorted list to be sorted by Merge Sort By Multi Processing:",end='\n')
    print(numList[:30],end='\n')
    print("",end='\n')
    print("Last 30 elements of the unsorted list to be sorted by Merge Sort By Multi Processing:",end='\n')
    print(numList[-30:],end='\n')
    print("",end='\n')
    sorter = Sorter()
    start = start = time.time()
    sorted_list = sorter.mergeSortByMultiProcessing(numList, numOfThreads)
    end = end = time.time()
    print("Merge Sort By Multi Processing: " + str((end - start)) + " seconds.")
    print("Sorted " + str(len(sorted_list)) + " items.")
    print("First 30 elements of the sorted list:")
    print(sorted_list[:30],end='\n')
    print("",end='\n')
    print("Last 30 elements of the sorted list:",end='\n')
    print(sorted_list[-30:],end='\n')
    print("",end='\n')

    print("---------------------------------------",end='\n')

    numList = []
    f = open ('numbers.txt', 'r')
    for line in f.readlines():
        numList.append(int(line))
    f.close()

    print("First 30 elements of the unsorted list to be sorted by Normal Python List Sort:")
    print(numList[:30])
    print("",end='\n')
    print("Last 30 elements of the unsorted list to be sorted by Normal Python List Sort:")
    print(numList[-30:])
    print("",end='\n')
    start = start = time.time()
    numList.sort()
    sorted_list = numList
    end = end = time.time()
    print("Normal Python List Sort: " + str((end - start)) + " seconds.",end='\n')
    print("Sorted " + str(len(sorted_list)) + " items.",end='\n')
    print("First 30 elements of the sorted list:",end='\n')
    print(sorted_list[:30],end='\n')
    print("",end='\n')
    print("Last 30 elements of the sorted list:",end='\n')
    print(sorted_list[-30:],end='\n')
    print("",end='\n')