import matplotlib.pyplot as plt
def plot_metric(x, y, yerr, title, ylabel):
    plt.figure()
    plt.errorbar(x, y, yerr=yerr, marker='o', linestyle='-')
    plt.xticks(rotation=45)
    plt.title(title)
    plt.xlabel("Workload")
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.show()

plot_metric(workloads, cycles, cycles_err, "GET: CPU Cycles", "Cycles")
plot_metric(workloads, instructions, instructions_err, "GET: Instructions", "Instructions")
plot_metric(workloads, cache_refs, cache_refs_err, "GET: Cache References", "Cache Refs")
plot_metric(workloads, cache_miss_pct, cache_miss_pct_err, "GET: Cache Miss Rate", "Miss Rate (%)")
plot_metric(workloads, elapsed, elapsed_err, "GET: Elapsed Time", "Seconds")
plot_metric(workloads, user_time, user_time_err, "GET: User CPU Time", "Seconds")
plot_metric(workloads, sys_time, sys_time_err, "GET: System CPU Time", "Seconds")
