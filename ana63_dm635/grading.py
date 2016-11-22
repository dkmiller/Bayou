#!/usr/bin/env python

import os, time, sys
from os.path import isfile, join
import shutil

def split_log(log, sp=','):
    ret = []
    valid_comma = True
    start = 0
    for i in range(len(log)):
        if valid_comma and log[i] == sp:
            ret.append(log[start:i])
            start = i + 1
        if log[i] == '(':
            valid_comma = False
        if log[i] == ')':
            valid_comma = True
    ret.append(log[start:])
    return ret

def clean_splitted_log_to_map(arr):
    ret = {}
    for s in arr:
        w = s
        sp = split_log(s, ':')
        if len(sp) != 3:
            continue
        if sp[0] == 'CREATE' or sp[0] == 'RETIRE':
            w = sp[0] + ':' + sp[2]
        if w not in ret:
            ret[w] = 1
        else:
            ret[w] += 1
    return ret

def check_map_equality(m1, m2):
    if len(m1) != len(m2):
        return False
    for k in m1:
        if k not in m2:
            return False
        if m1[k] != m2[k]:
            return False
    return True

def check(std, out):
    std_sp = split_log(std)
    out_sp = split_log(out)
    if len(std_sp) != len(out_sp):
        return False
    std_map = clean_splitted_log_to_map(std_sp)
    out_map = clean_splitted_log_to_map(out_sp)
    return check_map_equality(std_map, out_map)

os.system('./stopall >/dev/null 2>/dev/null')
os.system('./build >/dev/null 2>/dev/null')

test_output = 'test_output'
tests = 'tests'
if len(sys.argv) == 2:
    tests = sys.argv[1]
try:
    shutil.rmtree(test_output)
except:
    pass
os.mkdir(test_output)
for f in os.listdir(tests):
    abs_f = join(tests, f)
    if isfile(abs_f):
        if f[len(f) - len('.input'):] == '.input':
            fn = f[:len(f) - len('.input')]
            print fn,
            os.system('./master.py < ' + abs_f + \
                    ' 2> ' + join(test_output, fn+'.err') +\
                    ' > ' + join(test_output, fn+'.output'))

            os.system('./stopall >/dev/null 2>/dev/null')
            os.system("ps aux | grep -i java | awk '{print $2}' | xargs kill -9 >/dev/null 2>/dev/null")
            os.system("ps aux | grep -i process | awk '{print $2}' | xargs kill -9 >/dev/null 2>/dev/null")
            os.system("ps aux | grep -i python | grep -v grading | awk '{print $2}' | xargs kill -9 >/dev/null 2>/dev/null")

            with open(join(test_output, fn+'.output')) as fi:
                    out = fi.read().strip().split('\n')
            with open(join(tests, fn+'.output')) as fi:
                    std = fi.read().strip().split('\n')
            result = True
            out_index = 0
            stop = False
            for s in std:
                if stop:
                    break
                sp = s.split(None, 1)
                if sp[0] == 'getResp':
                    if out[out_index] != s:
                        result = False
                        break
                    out_index += 1
                else:
                    json = eval(sp[1])
                    prev = None
                    for i in range(json['count']):
                        # check the number of lines in output
                        if out_index >= len(out):
                            result = False
                            stop = True
                            break
                        # check if all the outputs are identical
                        if prev is not None and prev != out[out_index]:
                            result = False
                            stop = True
                            break
                        # check if the output is valid
                        out_log = out[out_index].split(None, 1)
                        if len(out_log) != 2:
                            result = False
                            stop = True
                            break
                        out_log = out_log[1]
                        if prev == None and check(json['log'], out_log) == False:
                            result = False
                            stop = True
                            break
                        prev = out[out_index]
                        out_index += 1

            if result:
                print 'correct'
            else:
                print 'wrong'
            time.sleep(2)
