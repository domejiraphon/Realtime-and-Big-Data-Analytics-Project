import matplotlib.pyplot as plt
from loguru import logger
import argparse
import numpy as np 

parser = argparse.ArgumentParser()
parser.add_argument('--filename', type=str, default="../out.txt")

args = parser.parse_args()
def isfloat(num):
  try:
    float(num)
    return True
  except ValueError:
    return False

def readFile():
  file1 = open(args.filename, 'r')
  data = []
  while True:
    line = file1.readline()
    if not line:
      break
    cur = line.strip().split(",")
    for i in range(len(cur)):
      if isfloat(cur[i]):
        cur[i] = float(cur[i])
    data.append(cur)
  data = np.array(data)
  return data 

def visualize(data):
  
  t1 = [i for i in range(0, data.shape[0], 1)]
  t2 = [int(float(i)) for i in data[:, 1]]
  fig = plt.figure()
  ax = fig.add_subplot()
  plt.plot(t1, t2)
  xs = np.linspace(0, data.shape[0], 6).astype(int)
  labels = ["2020-03", "2020-09", "2021-03", "2021-09", "2022-03", "2022-09"]
  plt.xticks(xs, labels)
  plt.xlabel("Date")
  plt.ylabel("Cases")
  plt.title("Covid daily casess")
 
  plt.savefig("./nvidia-smi.jpg")
  plt.clf()
@logger.catch
def main():
  data = readFile()
  visualize(data)
if __name__ == "__main__":
  main()