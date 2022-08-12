#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 10 16:00:35 2022

@author: maheshvakkund
"""
if __name__ == '__main__':
    d = {'employee_number': 1, 'employee_name': 'mahesh', 'city': 'delhi', 'sal': 89000}
    print(d)
    print(d['employee_number'])
    d['employee_name'] = 'Shamanth'
    print(d)
    del d['city']
    print(d)

    d = {'employee_number': 1, 'employee_name': 'mahesh', 'city': 'delhi', 'sal': 89000,
         'employee_name': 'Shamanth'}
    print(d)

    d['grade'] = 'A'
    print(d)

    e = {'state': 'delhi', 'country': 'india'}
    # Merge two dictionaries.
    d.update(e)
    print(d)

    print(d.get('employee_numbers', 'N/A'))

    print(d.keys())
    print(d.values())

    d = {'emp_no': [1, 2, 3, 4, 5],
         'emp_name': ['a', 'b', 'c', 'd', 'e']}
    print(d)
    print(d['emp_name'])

    d.clear()
    print(d)
