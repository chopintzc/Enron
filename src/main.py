'''
Created on Nov 25, 2016

@author: Zhongchao
'''

import os
from os import listdir, chdir
from discovery import mainFunc


path = "D:/course/bigdata/Assign/ass3/maildir/"

'''
get_immediate_subdirectories
'''
def get_immediate_subdirectories(a_dir):
    return [name for name in os.listdir(a_dir)
            if os.path.isdir(os.path.join(a_dir, name))]

'''
hasfolder
'''
def hasfolder(a_dir):
    d = get_immediate_subdirectories(a_dir)
    flag = False
    for curd in d:
        if curd == "all_documents":
            flag = True
            break
    return flag
          

'''
traverse all the subdirectories
'''
d = get_immediate_subdirectories(path)

for curd in d:
    subpath = path + "/" + curd
    if hasfolder(subpath):
        mainFunc(curd)
    