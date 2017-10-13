import gzip
import pandas
import json
import sys


example_json = sys.argv[1]
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


keys = [key.strip() for key in open(sys.argv[2]).readlines() if key[0] != '#']
#print(keys)
new_df = []
count = 0
for line in gzip.open(example_json,'rb'):
    trimed_dict = {}
    dict_ = (json.loads(line))
    flattten_dict = flatten_json(dict_)
    for key in flattten_dict:
        if key in keys:
            trimed_dict[key] = flattten_dict[key] 
    new_df.append(trimed_dict)
    count += 1
    #if count > 1000:
    #    break
df = pandas.DataFrame(new_df)
#df.to_csv('X.csv',compression='gzip')
df.to_csv("{}".format(sys.argv[3])+'.gz',compression='gzip')