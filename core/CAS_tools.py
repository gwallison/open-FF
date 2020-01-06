# -*- coding: utf-8 -*-
"""
Created on Sun Jan 20 11:14:52 2019

@author: GWAllison

collection of tools for working with CAS RNs
"""
import re
import pandas as pd

refdir = './sources/'

def is_valid_CAS_code(cas):
    """checks if number follows strictest format of CAS registry numbers
    three sections separated by '-', section 1 is 2-7 digits with no 
    leading zeros, section 2 is two digits (no dropping leading zero),
    check digit is just one digit that satisfies validation algorithm.
    No extraneous characters."""
    try:
        for c in cas:
            err = False
            if c not in ['0','1','2','3','4','5','6','7','8','9','-']: 
                err = True
                break
        if err: return False
        lst = cas.split('-')
        if len(lst)!=3 : return False
        if len(lst[2])!=1 : return False # check digit must be a single digit
        if lst[0][0] == '0': return False # leading zeros not allowed
        s1int = int(lst[0])
        if s1int > 9999999: return False
        if s1int < 10: return False
        s2int = int(lst[1])
        if s2int > 99: return False
        if len(lst[1])!=2: return False # must be two digits, even if <10

        # validate test digit
        teststr = lst[0]+lst[1]
        teststr = teststr[::-1] # reverse for easy calculation
        accum = 0
        for i,digit in enumerate(teststr):
            accum += (i+1)*int(digit)
        if accum%10 != int(lst[2]):
            return False
        return True
    except:
        return False

def is_valid_without_junk(cas):
    # first removes all non-digit and '-' characters.  Then returns value
    #  from is_valid_cas_code.
    try:
        cas = re.sub(r'[^0-9-]','',cas)
        #print(cas)
        return is_valid_CAS_code(cas)
    except:
        return False

def correct_zeros(cas):
    if is_valid_CAS_code(cas): # don't need to do anything
        return cas
    lst = cas.split('-')
    if len(lst) != 3: return cas
    if len(lst[2])!= 1: return cas # can't do anything here with malformed checkdigit
    if len(lst[1])!=2:
        if len(lst[1])==1:
            lst[1] = '0'+lst[1]
        else:
            return cas # wrong number of digits in chunk2 to fix here
    lst[0] = lst[0].lstrip('0')
    if (len(lst[0])<2 or len(lst[0])>7): return cas
   
    return f'{lst[0]}-{lst[1]}-{lst[2]}'
    

def get_proprietary_labels(source='./sources/'):
    # read file of lables used for proprietary and return list.
    fn = source+'CAS_labels_for_proprietary.csv'
    df = pd.read_csv(fn,names=['CAS_RN','counts','label'])
    return list(df.CAS_RN)


if __name__ == '__main__':
    print(is_valid_CAS_code('10049-04-14'))