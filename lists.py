#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 10 15:17:07 2022

@author: maheshvakkund
"""
if __name__ == '__main__':
    x = [32, 54, 'ABC', True, 90.3, False]
    print(x)
    print(x[1])
    x[0] = 23000
    print(x)
    del x[2]
    print(x)

    x = [['a', 76], [54, 90], [43, 77], ['aa', 'mm']]
    print(x)
    print(x[2])
    print(len(x))
    print(x[2:5])

    x = [32, 31, 90, 91, 9, 2, 1]
    print(max(x))
    print(min(x))
    x.sort()
    print(x)
    x.reverse()
    print(x)

    x = [32, 31, 90, 91, 9, 2, 1]
    y = sorted(x)
    print(x)
    print(y)

    y = sorted(x, reverse=True)
    print(y)

    x.append("ABC")
    x.append("ABC")
    print(x)
    x.insert(1, 'test')
    print(x)

    print(x.count('ABC'))

    x = ['ABC', 'XYX', 'TEST', 'ABC']
    y = [1, 'python', 'object']
    x.extend(y)
    print(x)
    print(x.index('ABC'))
    print(x.pop())
    print(x)

    print(x.remove('ABC'))
    print(x)
    x.clear()
    print(x)

    x = ['ABC', 'XYX', 'TEST', 'ABC', [1, 2, 3, 4]]
    print(list(enumerate(x)))

    for z, y in enumerate(x):
        print("index of", y, "is", z)
