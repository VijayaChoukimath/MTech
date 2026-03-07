import time

def run_filter_test(df):
    start = time.time()
    df.filter("popularity > 80").count()
    end = time.time()
    return end - start