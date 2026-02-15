import csv
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import os

CSV_PATH = 'bench_sustained_metrics_round2.csv'

if not os.path.exists(CSV_PATH):
    print(f'CSV not found: {CSV_PATH}')
    raise SystemExit(1)

times = []
alloc = []
heap_alloc = []
goroutines = []
num_gc = []
pause_ns = []

with open(CSV_PATH, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # time format is RFC3339
        t = datetime.fromisoformat(row['time'])
        times.append(t)
        alloc.append(int(row['alloc']) / (1024.0*1024.0))
        heap_alloc.append(int(row['heap_alloc']) / (1024.0*1024.0))
        goroutines.append(int(row['num_goroutine']))
        num_gc.append(int(row['num_gc']))
        pause_ns.append(int(row['pause_total_ns']) / 1e6)  # ms

# ensure output dir
OUT_DIR = '.'

# Plot Alloc and HeapAlloc
plt.figure(figsize=(12,6))
plt.plot(times, alloc, label='Alloc (MB)')
plt.plot(times, heap_alloc, label='HeapAlloc (MB)', alpha=0.8)
plt.xlabel('Time')
plt.ylabel('MB')
plt.title('Alloc / HeapAlloc over time')
plt.legend()
plt.tight_layout()
alloc_png = os.path.join(OUT_DIR, 'bench_alloc.png')
plt.savefig(alloc_png)
plt.close()
print('wrote', alloc_png)

# Plot Goroutines
plt.figure(figsize=(12,4))
plt.plot(times, goroutines, label='Goroutines')
plt.xlabel('Time')
plt.ylabel('Count')
plt.title('Goroutines over time')
plt.tight_layout()
gor_png = os.path.join(OUT_DIR, 'bench_goroutines.png')
plt.savefig(gor_png)
plt.close()
print('wrote', gor_png)

# Plot NumGC
plt.figure(figsize=(12,4))
plt.plot(times, num_gc, label='NumGC')
plt.xlabel('Time')
plt.ylabel('Count')
plt.title('NumGC over time')
plt.tight_layout()
gc_png = os.path.join(OUT_DIR, 'bench_numgc.png')
plt.savefig(gc_png)
plt.close()
print('wrote', gc_png)

# Plot PauseTotal (ms)
plt.figure(figsize=(12,4))
plt.plot(times, pause_ns, label='PauseTotal (ms)')
plt.xlabel('Time')
plt.ylabel('ms')
plt.title('GC PauseTotal over time')
plt.tight_layout()
pause_png = os.path.join(OUT_DIR, 'bench_pause.png')
plt.savefig(pause_png)
plt.close()
print('wrote', pause_png)
