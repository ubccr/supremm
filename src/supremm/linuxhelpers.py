#!/usr/bin/env python
""" Helper functions that can process data that is generated on 
    resources that use a Linux kernel or Linux based OS."""


def parsecpusallowed(cpusallowed):
    """ cpusallowed parser converts the human-readable cpuset string to
        a list of cpu indexes
    """

    cpulist = set()
    items = cpusallowed.split(",")
    for item in items:
        try:
            cpulist.add(int(item))
        except ValueError as e:
            try:
                cpurange = [int(x) for x in item.split("-")]
                if len(cpurange) != 2:
                    raise ValueError("Unable to parse cpusallowed \"" + cpusallowed + "\"")
                cpulist |= set(range(cpurange[0], cpurange[1] + 1))
            except ValueError as e:
                raise ValueError("Unable to parse cpusallowed \"" + cpusallowed + "\"")

    return cpulist


if __name__ == "__main__":
    print parsecpusallowed("0-7")
    print parsecpusallowed("1")
    print parsecpusallowed("1,2")
    print parsecpusallowed("1,2,4-6,15")
    print parsecpusallowed("1,6-7")
    print parsecpusallowed("6-7,9")
