import subprocess
import os

script_dir = os.path.dirname(os.path.abspath(__file__))

# Set environment variables to avoid issues with spaces in paths
env = os.environ.copy()
env['TEMP'] = 'C:\\Temp'
env['TMP'] = 'C:\\Temp'

print("Running Spotify Storage Benchmark")

subprocess.run([r"C:\sparkenv\Scripts\python.exe", os.path.join(script_dir, "write_parquet.py")], env=env, cwd=script_dir)
subprocess.run([r"C:\sparkenv\Scripts\python.exe", os.path.join(script_dir, "write_orc.py")], env=env, cwd=script_dir)
subprocess.run([r"C:\sparkenv\Scripts\python.exe", os.path.join(script_dir, "write_avro.py")], env=env, cwd=script_dir)