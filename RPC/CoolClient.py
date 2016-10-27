import xmlrpc.client
import math


def merge(l, m):
    result = []
    i = j = 0
    total = len(l) + len(m)
    while len(result) != total:
        if len(l) == i:
            result += m[j:]
            break
        elif len(m) == j:
            result += l[i:]
            break
        elif l[i] < m[j]:
            result.append(l[i])
            i += 1
        else:
            result.append(m[j])
            j += 1
    return result

def main():
    host = '127.0.0.1'

    array = []
    sorted_part1 = []
    sorted_part2 = []

    f = open('small_numbers.txt', 'r')
    for line in f.readlines():
        array.append(int(line))
    f.close()

    part1 = array[:math.floor(len(array)/2)]
    part2 = array[math.floor(len(array)/2):]
    print(type(part1))
    port = 5001
    proxy = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
    #print('Ping first remote server:', ping(proxyFirst))
    sorted_part1 = proxy.sort_array(part1)

    port = 5002
    proxySecond = xmlrpc.client.ServerProxy("http://" + host + ":" + str(port) + "/")
    #print('Ping second remote server:', ping(proxyFirst))

    sorted_part2 = proxySecond.sort_array(part2)
    print(sorted_part2)
    print("------------------")

    print(merge(sorted_part1, sorted_part2))

    # merged_list = []
    # for item in heapq.merge(sorted_part1,sorted_part2):
    #     merged_list.append(item)
    # print(merged_list)


if __name__ == '__main__':
    main()


