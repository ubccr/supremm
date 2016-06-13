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
        "simdins": {
            "units": "insts/s",
            "description": "SIMD instructions",
            "help": "The total rate of floating point SIMD instructions reported by the hardware performance counters on the CPU cores on which the job ran. Note that the meaning of this value is hardware-specific so the data should not in general be compared between HPC resources that have different hardware architectures."
        },
        "memused_minus_diskcache": {
            "units": "GB",
            "description": "Node Memory RSS",
            "help": "The total physical memory used by the operating system excluding memory used for caches. This value includes the contribution for <em>all</em> processes including system daemons and all running HPC jobs but does not include the physical memory used by the kernel page and SLAB caches. For HPC resources that use a Linux-based operating system this value is obtained from the <code>meminfo</code> file in sysfs for each numa node (i.e. <code>/sys/devices/system/node/nodeX/meminfo</code>)",
        },
        "memused": {
            "units": "GB",
            "description": "Total Node Memory",
            "help": "The total physical memory used by the operating system. For HPC resources that use a Linux-based operating system this value is obtained from the <code>meminfo</code> file in sysfs for each numa node (i.e. <code>/sys/devices/system/node/nodeX/meminfo</code>)"
        },
        "process_mem_usage" : {
            "units" : "GB",
            "description" : "Total CGroup Memory",
            "help" : "The total amount of memory used in the memory cgroup that contained the job. The value is obtained from the kernel cgroup metrics."
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
        "block": {
            "units": "MB/s",
            "description": "Block Filesystem traffic",
            "help": "The total rate of data transferred to and from the block devices on each node.  The rate is computed over each time interval and is the sum of data read and written."
        },
        "nfs" : {
            "units" : "MB/s",
            "description" : "NFS Filesystem traffic",
            "help" : "The total rate of data transferred to and from the parallel filesystem over NFS mounts. The rate is computed over each time interval and is the sum of data sent and received by each node."
        }
    }
};

var summarydef = {
    "summary_version": "summary-1.0.5", 
    "_id": "summary-1.0.5",
    "definitions": {
        "lnet": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "catastrophe": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "infiniband": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "cpuperf": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "lustre": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "gpfs": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "ib_lnet": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "memused_minus_diskcache": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "cpuuser": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "process_memory": {
            "usage": {
                "avg": {
                    "documentation": "The average amount of memory used in the memory cgroup that contained the job. The value is obtained from the kernel cgroup metrics. The average is calculated as the mean value of each memory usage measurement.", 
                    "type": "instant", 
                    "unit": "byte"
                },
                "max": {
                    "documentation": "The maximum value of the process memory on a node.",
                    "type": "instant",
                    "unit": "byte"
                }
            },
            "usageratio": {
                "avg": {
                    "documentation": "The average ratio of memory used to the memory limit for the processes in the memory cgroup that contained the job. The value is obtained from the kernel cgroup metrics.", 
                    "type": "instant", 
                    "unit": "ratio"
                },
                "max": {
                    "documentation": "The maximum value of the process memory on a node.",
                    "type": "instant",
                    "unit": "byte"
                }
            },
            "limit": {
                "documentation": "The memory limit for the memory cgroup that contained the job. The value is obtained from the kernel cgroup metrics.",
                "type": "instant",
                "unit": "byte"
            }
        }, 
        "nfs": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "simdins": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "uncperf": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "memory": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "process_mem_usage": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "gpu": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "proc": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "cpu": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "block": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "network": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }
    } 
};

db = db.getSiblingDB("supremm");
db.schema.update({_id: sdef._id}, sdef, {upsert: true})
db.schema.update({_id: summarydef._id}, summarydef, {upsert: true})
