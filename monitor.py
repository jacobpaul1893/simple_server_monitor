#!/usr/bin/env python3
import os
import time
import psutil
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

def get_cpu_usage():
    cpu_usage = psutil.cpu_percent(interval=1)
    data = {"Time": pd.Timestamp.now(), "CPU%": cpu_usage}
    df = pd.json_normalize(data)
    return df

def get_mem_usage():
    mem_usage = psutil.virtual_memory().percent
    data = {"Time": pd.Timestamp.now(), "MEM%": mem_usage}
    df = pd.json_normalize(data)
    return df

def get_load_usage():
    load = psutil.getloadavg()[0] / psutil.cpu_count() * 100
    data = {"Time": pd.Timestamp.now(), "LOAD%": load}
    df = pd.json_normalize(data)
    return df

def write_data(file, fn):
    df_new = fn()
    if os.path.isfile(file):
        df_existing = pd.read_parquet(file)
        df_final = df_existing.append(df_new, ignore_index=True)
        table = pa.Table.from_pandas(df_final)
        pqwriter = pq.ParquetWriter(file, table.schema)
        pqwriter.write_table(table)
        pqwriter.close()
        return True
    df_new.to_parquet(file)
    return True

def monitor():
    data_dir = "data"
    if not os.path.isdir(data_dir):
        os.mkdir(data_dir)
    cpu_file = "data/cpu_usage.parquet"
    mem_file = "data/mem_usage.parquet"
    load_file = "data/load_usage.parquet"

    write_data(cpu_file, get_cpu_usage)
    write_data(mem_file, get_mem_usage)
    write_data(load_file, get_load_usage)

if __name__ == "__main__":
    while True:
        monitor()
        time.sleep(5)

