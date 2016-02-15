import os
import json

def path_to_dict(path):
    d = {'name': os.path.basename(path)}
    if os.path.isdir(path):
        d['type'] = "directory"
        d['children'] = [path_to_dict(os.path.join(path,x)) for x in os.listdir\
(path)]
    else:
        d['type'] = "file"
        d['children'] = []
    return d

d_tree = path_to_dict('/Users/kerfoot/datasets/ooi/uframe/tds_home/historical/nc')
