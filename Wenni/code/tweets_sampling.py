# import required module
import os
import time
import pandas as pd
import random
import numpy as np
# assign directory
directory = 'corona_tweets_901-927'
# iterate over files in
# that directory
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    # checking if it is a file
    if os.path.isfile(f):
        data=pd.read_csv(f, header=None)
        sample=[]
        while (len(sample)<50000):
            index=random.randint(0,len(data)-1)
            if index in sample:
                continue
            else:
                sample.append(index)
                data.iloc[[index]].to_csv("sampled_tweets_901-927.csv", mode='a',header=False,index=False)
    print(filename+" is done!")