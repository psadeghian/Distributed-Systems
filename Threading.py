import heapq, threading, multiprocessing, time

class Sorter:
    """Class for sorting a list of numbers using threading or multi processing. """
    def __init__(self):
        self.list_of_sorted_lists = []

    # def merge(*iterables):
    #     h = []
    #     for it in map(iter, iterables):
    #         try:
    #             next = it.__next__
    #             h.append([next(), next])
    #         except StopIteration:
    #             pass
    #     heapq.heapify(h)
    #
    #     while True:
    #         try:
    #             while True:
    #                 v, next = s = h[0]
    #                 yield v
    #                 s[0] = next()
    #                 heapq._siftup(h, 0)
    #         except StopIteration:
    #             heapq.heappop(h)
    #         except IndexError:
    #             return

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
        print(len(lyst))
        print(lyst[-20:])
        print("-------------")
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
        list_of_sorted_lists = multiprocessing.Value()
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

    sorter = Sorter()
    start = start = time.time()
    sorted_list = sorter.mergeSortByThreading(numList, numOfThreads)
    print(sorted_list[-100:])
    end = end = time.time()
    print("Merge Sort By Threading: " + str((end - start)) + " seconds.")
    print(str(len(sorted_list)) + " items.")

    numList = []
    f = open ('numbers.txt', 'r')
    for line in f.readlines():
        numList.append(int(line))
    f.close()

    sorter = Sorter()
    start = start = time.time()
    sorted_list = sorter.mergeSortByMultiProcessing(numList, numOfThreads)
    print(sorted_list[-100:])
    end = end = time.time()
    print("Merge Sort By Multi Processing: " + str((end - start)) + " seconds.")
    print(str(len(sorted_list)) + " items.")

    numList = []
    f = open ('numbers.txt', 'r')
    for line in f.readlines():
        numList.append(int(line))
    f.close()

    start = start = time.time()
    numList.sort()
    sorted_list = numList
    print(sorted_list[-100:])
    end = end = time.time()
    print("Normal Python List Sort: " + str((end - start)) + " seconds.")
    print(str(len(sorted_list)) + " items.")
