import time
from functools import wraps
import threading
import psutil
import pandas as pd
import csv
import os


def fn_timer(function):
    """
    Definition of decorator measuring time of function executing
    :param function: function to decorate
    :return: time of function executing
    """

    @wraps(function)
    def function_timer(*args, **kwargs):
        start_time = time.time()
        start_time2 = time.process_time_ns()
        result = function(*args, **kwargs)
        end_time = time.time()
        end_time2 = time.process_time_ns()
        path = "measures.csv"
        exists = os.path.exists(path)
        with open("measures.csv", "a", newline='') as f:
            writer = csv.writer(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            if not exists:
                writer.writerow(["Execution Time", "CPU Usage"])
            f.write(str(end_time - start_time) + ',' + str(end_time2 - start_time2) + '\n')
        # print(f"Runtime of {function.__name__} is {end_time - start_time:.04} seconds.")
        return result

    return function_timer

def fn_cpu_time(function):
    @wraps(function)
    def function_cpu_time(*args, **kwargs):
        start_time = time.process_time_ns()
        result = function(*args, **kwargs)
        end_time = time.process_time_ns()
        path = "measures.csv"
        exists = os.path.exists(path)
        with open("measures.csv", "a", newline='') as f:
            #writer = csv.writer(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
            # if not exists:
            #     writer.writerow(["Execution Time", "CPU Usage"])
            f.write(str(end_time - start_time) + '\n')
        # print(f"Runtime of {function.__name__} is {end_time - start_time:.04} seconds.")
        return result

    return function_cpu_time



class DisplayCPU(threading.Thread):

    def run(self):
        self.running = True
        current_process = psutil.Process()
        df = pd.DataFrame(columns=['CPU', 'Virtual_memory'])
        i = 0
        while self.running:
            df.loc[i] = [
                current_process.cpu_percent(interval=None),
                psutil.virtual_memory()[2]
            ]
            i += 1
        df.drop(index=df.index[-1], axis=0, inplace=True)
        df_mean_cpu = df[['CPU']].mean()
        df_mean_ram = df[['Virtual_memory']].mean()
        df_mean_cpu.to_csv('measures.csv', mode='a', sep=',', lineterminator=",", encoding='utf-8', index=False,
                           header=False)
        df_mean_ram.to_csv('measures.csv', mode='a', sep='\n', encoding='utf-8', index=False, header=False)
        # print("df: ", df)
        # print("df_mean: ", df_mean)
        return df

    def stop(self):
        self.running = False


def fn_cpu_memory_usage(function):
    @wraps(function)
    def function_cpu_usage(*args, **kwargs):
        display_cpu = DisplayCPU()
        display_cpu.start()
        try:
            result = function(*args, **kwargs)
        finally:
            display_cpu.stop()
        return result

    return function_cpu_usage
