#!/usr/bin/env python

from supremm.plugin import DeviceInstanceBasedPlugin

class BlueWaters(DeviceInstanceBasedPlugin):
    """ Process bluewaters-specific metrics """

    name = property(lambda x: "bluewaters")
    requiredMetrics = property(lambda x: [
        "bluewaters.SMSG_ntx",
        "bluewaters.SMSG_tx_bytes",
        "bluewaters.SMSG_nrx",
        "bluewaters.SMSG_rx_bytes",
        "bluewaters.RDMA_ntx",
        "bluewaters.RDMA_tx_bytes",
        "bluewaters.RDMA_nrx",
        "bluewaters.RDMA_rx_bytes"
        ])
    optionalMetrics = property(lambda x: [])
    derivedMetrics = property(lambda x: [])

