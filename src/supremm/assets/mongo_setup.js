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
        "sveins": {
            "units": "insts/s",
            "description": "SVE instructions",
            "help": "The total rate of SVE instructions reported by the hardware performance counters on the CPU cores on which the job ran. The number of floating point operations per SVE instruction retired can vary."
        },
        "gpu_usage": {
            "units": "GPU %",
            "description": "GPU utilization %",
            "help": "The average percentage of time spent with the GPU active. The average is computed over each time interval."
        },
        "clktks": {
            "units": "insts/s",
            "description": "Clock Ticks",
            "help": "The total rate of clock ticks reported by the hardware performance counters on the CPU cores on which the job ran. Note that the meaning of this value is hardware-specific so the data should not in general be compared between HPC resources that have different hardware architectures."
        },
        "memused_minus_diskcache": {
            "units": "GB",
            "description": "Node Memory RSS",
            "help": "The total physical memory used by the operating system excluding memory used for caches. This value includes the contribution for <em>all</em> processes including system daemons and all running HPC jobs but does not include the physical memory used by the kernel page and SLAB caches. For HPC resources that use a Linux-based operating system this value is obtained from the <code>meminfo</code> file in sysfs for each numa node (i.e. <code>/sys/devices/system/node/nodeX/meminfo</code>)"
        },
        "corepower": {
            "units": "Watts",
            "help": "The power consumption obtained from hardware measurements built into the CPU or motherboard. This will not include the contribution from other hardware components on the compue node such as periperal cards and power supplies.",
            "description": "CPU Power"
        },
        "power": {
            "units": "Watts",
            "help": "The power consumption of the compute nodes obtained from the IPMI DCMI interface. This does not include any associated power usage from other datacenter components such as network switches, parallel filesystems and cooling.",
            "description": "Node Power"
        },
        "memused": {
            "units": "GB",
            "description": "Total Node Memory",
            "help": "The total physical memory used by the operating system. For HPC resources that use a Linux-based operating system this value is obtained from the <code>meminfo</code> file in sysfs for each numa node (i.e. <code>/sys/devices/system/node/nodeX/meminfo</code>)"
        },
        "process_mem_usage": {
            "units": "GB",
            "description": "Total CGroup Memory",
            "help": "The total amount of memory used in the memory cgroup that contained the job. The value is obtained from the kernel cgroup metrics."
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
            "units": "GB/s",
            "description": "Block Filesystem traffic",
            "help": "The total rate of data transferred to and from the block devices on each node.  The rate is computed over each time interval and is the sum of data read and written."
        },
        "nfs": {
            "units": "MB/s",
            "description": "NFS Filesystem traffic",
            "help": "The total rate of data transferred to and from the parallel filesystem over NFS mounts. The rate is computed over each time interval and is the sum of data sent and received by each node."
        }
    }
};

var summarydef = {
    "summary_version": "summary-1.0.6", 
    "_id": "summary-1.0.6",
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
            "cpiref": {
                "documentation": "The average clock ticks per instruction for each core.",
                "type": "ratio",
                "unit": "ratio"
            },
            "cpldref": {
                "documentation": "The average clock ticks per L1D cache load for each core.",
                "type": "ratio",
                "unit": "ratio"
            },
            "flops": {
                "documentation": "The number of floating point instructions executed per core.",
                "type": "instant",
                "unit": "op"
            }
        }, 
        "load1": {
            "max": {
                "documentation": "The maximum value of the 1-second 'load average' during the job.",
                "type": "instant",
                "unit": ""
            },
            "maxpercore": {
                "documentation": "The maximum value of the 1-second 'load average' per core during the job.",
                "type": "instant",
                "unit": ""
            },
            "mean": {
                "documentation": "The arithmetic mean of the 1-second 'load average' during the job.",
                "type": "instant",
                "unit": ""
            },
            "meanpercore": {
                "documentation": "The arithmetic mean of the 1-second 'load average' per core during the job.",
                "type": "instant",
                "unit": ""
            }
        },
        "lustre": {
            "*": {
                "read_bytes-total": {
                    "documentation": "",
                    "type": "instant",
                    "unit": "byte"
                },
                "write_bytes-total": {
                    "documentation": "",
                    "type": "instant",
                    "unit": "byte"
                }
            }
        }, 
        "gpfs": {
            "*": {
                "read": {
                    "documentation": "The number of read operations.",
                    "type": "instant",
                    "unit": ""
                },
                "write": {
                    "documentation": "The number of write operations",
                    "type": "instant",
                    "unit": ""
                },
                "read_bytes": {
                    "documentation": "The amount of data read.",
                    "type": "instant",
                    "unit": "byte"
                },
                "write_bytes": {
                    "documentation": "The amount of data written.",
                    "type": "instant",
                    "unit": "byte"
                }
            }
        }, 
        "nodememory": {
            "free": {
                "documentation": "The average amount of free memory per node for the job. The value is obtained from /proc/meminfo. The average is calculated as the mean value of each memory usage measurement.",
                "type": "instant",
                "unit": "kilobyte"
            },
            "maxfree": {
                "documentation": "The maximum value of the free memory on a node.",
                "type": "instant",
                "unit": "kilobyte"
            },
            "used": {
                "documentation": "The average amount of used memory per node.",
                "type": "instant",
                "unit": "kilobyte"
            },
            "maxused": {
                "documentation": "The maximum value of the used memory on a node.",
                "type": "instant",
                "unit": "kilobyte"
            },
            "physmem": {
                "documentation": "The total physical memory on a node.",
                "type": "instant",
                "unit": "kilobyte"
            }
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
                    "documentation": "The maximum value of the ratio of memory used to the memory limit for the processes in the memory cgroup that contained the job. The value is obtained from the kernel cgroup metrics.",
                    "type": "instant",
                    "unit": "ratio"
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
            "membw": {
                "documentation": "The average amount of data transferred to and from main memory per node.",
                "type": "instant",
                "unit": "byte"
            }
        }, 
        "memory": {
            "used": {
                "documentation": "The mean memory usage during the job. This value is the usage per core. This value includes the OS buffers and kernel slab cache.", 
                "type": "", 
                "unit": "kilobyte"
            },
            "used_minus_cache": {
                "documentation": "The mean memory usage during the job. This value is the usage per core.", 
                "type": "", 
                "unit": "kilobyte"
            }
        }, 
        "gpu": {
            "*": {
                "gpuactive": {
                    "documentation": "The average GPU usage.", 
                    "type": "instant", 
                    "unit": "%"
                },
                "gpuactivemax": {
                    "documentation": "The peak GPU usage.", 
                    "type": "instant", 
                    "unit": "%"
                },
                "memused": {
                    "documentation": "The average memory usage per GPU.", 
                    "type": "instant", 
                    "unit": "byte"
                },
                "memusedmax": {
                    "documentation": "The peak memory usage for each GPU.", 
                    "type": "instant", 
                    "unit": "byte"
                }
            }
        }, 
        "proc": {
            "documentation": "", 
            "type": "", 
            "unit": ""
        }, 
        "cpu": {
            "jobcpus": {
                "idle": {
                    "documentation": "The CPU usage in idle mode of the cores that were assigned to the job. This metric reports the overall usage of each core that the job was assigned rather than, for example, the CPU usage of the job processes themselves.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "iowait": {
                    "documentation": "The CPU usage in iowait mode of the cores that were assigned to the job. This metric reports the overall usage of each core that the job was assigned rather than, for example, the CPU usage of the job processes themselves.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "irq": {
                    "documentation": "The CPU usage in irq mode of the cores that were assigned to the job. This metric reports the overall usage of each core that the job was assigned rather than, for example, the CPU usage of the job processes themselves.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "nice": {
                    "documentation": "The CPU usage in nice mode of the cores that were assigned to the job. This metric reports the overall usage of each core that the job was assigned rather than, for example, the CPU usage of the job processes themselves.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "softirq": {
                    "documentation": "The CPU usage in softirq mode of the cores that were assigned to the job. This metric reports the overall usage of each core that the job was assigned rather than, for example, the CPU usage of the job processes themselves.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "system": {
                    "documentation": "The CPU usage in system mode of the cores that were assigned to the job. This metric reports the overall usage of each core that the job was assigned rather than, for example, the CPU usage of the job processes themselves.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "user": {
                    "documentation": "The CPU usage in user mode of the cores that were assigned to the job. This metric reports the overall usage of each core that the job was assigned rather than, for example, the CPU usage of the job processes themselves.",
                    "type": "instant",
                    "unit": "ratio"
                }
            },
            "nodecpus": {
                "idle": {
                    "documentation": "The CPU usage in idle mode of the compute nodes on which the job ran. This value includes the contribution from all cores on each compute node regardless of whether the job processes were assigned to or ran on them.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "iowait": {
                    "documentation": "The CPU usage in iowait mode of the compute nodes on which the job ran. This value includes the contribution from all cores on each compute node regardless of whether the job processes were assigned to or ran on them.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "irq": {
                    "documentation": "The CPU usage in irq mode of the compute nodes on which the job ran. This value includes the contribution from all cores on each compute node regardless of whether the job processes were assigned to or ran on them.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "nice": {
                    "documentation": "The CPU usage in nice mode of the compute nodes on which the job ran. This value includes the contribution from all cores on each compute node regardless of whether the job processes were assigned to or ran on them.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "softirq": {
                    "documentation": "The CPU usage in softirq mode of the compute nodes on which the job ran. This value includes the contribution from all cores on each compute node regardless of whether the job processes were assigned to or ran on them.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "system": {
                    "documentation": "The CPU usage in system mode of the compute nodes on which the job ran. This value includes the contribution from all cores on each compute node regardless of whether the job processes were assigned to or ran on them.",
                    "type": "instant",
                    "unit": "ratio"
                },
                "user": {
                    "documentation": "The CPU usage in user mode of the compute nodes on which the job ran. This value includes the contribution from all cores on each compute node regardless of whether the job processes were assigned to or ran on them.",
                    "type": "instant",
                    "unit": "ratio"
                }
            },
            "effectivecpus": {
                "user": {
                    "documentation": "The effective cpu metric reports the statistics of the subset of CPU cores that have an average usage above a threshold. The metric is intended to be used to distinguish cpu cores that are running user processes from those that are not. The threshold value is resource-specific.",
                    "type": "instant",
                    "unit": "ratio"
                }
            }
        },
        "block": {
            "*": {
                "read": {
                    "documentation": "The number of disk read operations.",
                    "type": "instant",
                    "unit": ""
                },
                "write": {
                    "documentation": "The number of disk write operations",
                    "type": "instant",
                    "unit": ""
                },
                "read_bytes": {
                    "documentation": "The amount of data read from disk.",
                    "type": "instant",
                    "unit": "byte"
                },
                "write_bytes": {
                    "documentation": "The amount of data written to disk.",
                    "type": "instant",
                    "unit": "byte"
                }
            }
        }, 
        "gemini": {
            "totalinput": {
                "documentation": "",
                "type": "instant",
                "unit": "byte"
            },
            "totaloutput_optA": {
                "documentation": "",
                "type": "instant",
                "unit": "byte"
            },
            "totaloutput_optB": {
                "documentation": "",
                "type": "instant",
                "unit": "byte"
            }
        },
        "network": {
            "*": {
                "in-bytes": {
                    "documentation": "",
                    "type": "instant",
                    "unit": "byte"
                },
                "out-bytes": {
                    "documentation": "",
                    "type": "instant",
                    "unit": "byte"
                }
            }
        },
        "cpucategories": {
            "documentation": "Categorization of the CPU utilization of the job as good, pinned, unpinned, or low. A job is good if every core is heavily utilized, and a job is low if the cores are barely utilized or not at all. A pinned job consists of a scenario where a subset of the cores does most of the work, while an unpinned job is when the work is spread unevenly across many cores.",
            "type": "",
            "unit": ""
        },
        "rapl": {
            "energy" : {
                "pkg" : {
                    "documentation": "",
                    "type": "",
                    "unit": "Joules"
                },
                "cores" : {
                    "documentation": "",
                    "type": "",
                    "unit": "Joules"
                },
                "dram" : {
                    "documentation": "",
                    "type": "",
                    "unit": "Joules"
                }
            },
            "power": {
                "pkg" : {
                    "documentation": "",
                    "type": "",
                    "unit": "Watts"
                },
                "cores" : {
                    "documentation": "",
                    "type": "",
                    "unit": "Watts"
                },
                "dram" : {
                    "documentation": "",
                    "type": "",
                    "unit": "Watts"
                }
            }
        }
    } 
};

db = db.getSiblingDB("supremm");
db.schema.update({_id: sdef._id}, sdef, {upsert: true});
db.schema.update({_id: summarydef._id}, summarydef, {upsert: true});
