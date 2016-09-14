import random

def main():
    howMany = 0
    numbers = []


    howMany = int(input('How many numbers would you like to generate?: '))

    infile = open ('small_numbers.txt', 'w')

    for n in range(1,howMany):
        numbers.append(random.randint(1,1000))
    infile.write('\n'.join(map(str, numbers)))
    infile.close()
main()
