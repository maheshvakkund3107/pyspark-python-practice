from functools import reduce


def function1():
    x = 10
    y = 20
    return x + y, x - y


def square():
    c = 5
    return lambda c: c ** 2


def addition_subtraction():
    a = 10
    b = 20
    return lambda a, b: (a + b, a - b)


if __name__ == '__main__':
    print(square())
    my_list = [1, 5, 4, 6, 8, 11, 3, 12, 14]
    my_new_list = filter(lambda x: (x % 2 == 0), my_list)
    print(my_new_list)
    my_new_list = list(map(lambda x: x * 2, my_list))
    print(my_new_list)
    print(function1())
    print(addition_subtraction())
    ab = reduce(lambda x, y: x * y, my_list)
    print(ab)
