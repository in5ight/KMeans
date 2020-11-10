import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

for x in range(2, 5):
    for j in range(3, 11):
        filepath = str(x) + '-' + str(j)

        print(filepath)
        file = open(filepath + '\\part-m-00000', 'r')
        try:
            text_lines = file.readlines()
            data = np.zeros(
                (len(text_lines), 1 + len(text_lines[0].split("\t")[0].split(","))))
            i = 0
            for line in text_lines:
                coordinate = line.split("\t")[0]
                mark = line.split("\t")[1]
                data[i, 0] = coordinate.split(",")[0]
                data[i, 1] = coordinate.split(",")[1]
                data[i, 2] = mark
                i += 1
        finally:
            file.close()

        df = pd.DataFrame(data)
        df = df.rename(columns={0: 'x', 1: 'y', 2: 'mark'})

        mark_num = len(df['mark'].unique())

        for i in range(mark_num):
            df_temp = df[df['mark'] == i + 1]
            plt.scatter(df_temp['x'], df_temp['y'])

        plt.savefig(filepath + '.png')
        print('finish' + filepath)
