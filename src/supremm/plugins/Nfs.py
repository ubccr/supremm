#!/usr/bin/env python3

from supremm.plugin import DeviceBasedPlugin

class Nfs(DeviceBasedPlugin):
    """ Generate usage statistics for NFS clients """

    name = property(lambda x: "nfs")
    requiredMetrics = property(lambda x: [
        "nfsclient.bytes.read.normal",
        "nfsclient.bytes.read.direct",
        "nfsclient.bytes.read.server",
        "nfsclient.bytes.write.normal",
        "nfsclient.bytes.write.direct",
        "nfsclient.bytes.write.server"
        ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])


