import random


def main():
    how_many = 0
    numbers = []

    how_many = int(input('How many numbers would you like to generate?: '))

    infile = open('test_bl.txt', 'w')

    for n in range(1, how_many+1):
        numbers.append(random.randint(1, 1000000))
    infile.write('\n'.join(map(str, numbers)))
    infile.close()

main()
