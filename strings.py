# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

# print("hello")
if __name__ == '__main__':
    # Variables.
    x = 5
    y = 10
    l = [1, 2, 3, 4, 5]
    x = l.reverse()
    type(x)
    c = [i for i in l if i % 2 == 0]
    a = 'this is my code'
    print(a[1])
    print(a[1:4])
    print(a[6:])
    print(a[::-1])

    print(a.capitalize())
    print(a.title())
    print(a.lower())
    print(a.upper())
    print(a.count('i'))
    print(a.index('h'))

    a = 'A1234'
    print(a.isalnum())
    print(a.isalpha())

    d = '3233'
    print(d.isdigit())

    s = 'hello'
    print(s.islower())
    print(s.isupper())

    a = 'this is my code'
    print(min(a))
    print(max(a))

    a = '-'
    s = ("Jan", "Feb", "Mar")
    print(a.join(s))

    a, b, c, d = 21, 'abc', True, 65.4
    print(a, b, c, d)

    # deallocation of resources.
    del a
