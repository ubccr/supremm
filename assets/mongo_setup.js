var sdef = {
    "_id": "timeseries-4",
    "type": "timeseries",
    "applies_to_version": 4,
    "metrics": {
        "cpuuser": {
            "units": "CPU %",
            "description": "CPU User",
            "help": "The average percentage of time spent in CPU user mode. The average is computed over each time interval."
        },
        "membw": {
            "units": "GB/s",
            "description": "Memory bandwidth",
            "help": "The total rate of data transferred to and from main memory. The rate is computed over each time interval. This value is obtained from the hardware counters."
        },
        "memused_minus_diskcache": {
            "units": "GB",
            "description": "Memory usage",
            "help": "The total amount of application memory allocated. The value computed as the total allocated memory minus the memory used by kernel page and SLAB caches."
        },
        "ib_lnet": {
            "units": "MB/s",
            "description": "Interconnect MPI traffic",
            "help": "The total rate of data transferred over the parallel interconnect. The rate is computed over each time interval and is the sum of the data sent and received by each node. Some HPC resources also use the interconnect for parallel filesystem traffic; this filesystem traffic is not included in these data."
        },
        "lnet": {
            "units": "MB/s",
            "description": "Parallel Filesystem traffic",
            "help": "The total rate of data transferred to and from the parallel filesystem. The rate is computed over each time interval and is the sum of data sent and received by each node."
        },
        "simdins": {
            "units": "insts/s",
            "description": "SIMD instructions",
            "help": "The total rate of floating point SIMD instructions reported by the hardware performance counters on the CPU cores on which the job ran. Note that the meaning of this value is hardware-specific so the data should not in general be compared between HPC resources that have different hardware architectures."
        }
    }
};

conn = new Mongo();
db = conn.getDB("supremm");
db.schema.insert(sdef)
