# pylint: disable=no-member,invalid-name
""" Wrapper module for pcpfast - the core Performace Co-Pilot API """
#
# Copyright (C) 2015 Joe White
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
# or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# for more details.
#

# constants adapted from C header file <pcp/pmapi.h>
import cpmapi as c_api

# for interfacing with LIBPCP - the client-side C API
from ctypes import c_int, c_char_p
from ctypes import CDLL, POINTER
from ctypes import addressof, sizeof, byref
from ctypes import memmove
from ctypes.util import find_library

from pcp import pmapi
import os

##############################################################################
#
# dynamic library loads
#

LIBPCPFAST = CDLL(os.path.join(os.path.dirname(os.path.abspath(__file__)), "libpcpfast.so"))
LIBC = CDLL(find_library("c"))


##############################################################################
#
# python version information and compatibility
#
import sys

if sys.version >= '3':
    integer_types = (int,)
    long = int
else:
    integer_types = (int, long,)


##############################################################################
#
# function prototypes
#

LIBPCPFAST.pcpfastExtractValues.restype = c_int
LIBPCPFAST.pcpfastExtractValues.argtypes = [POINTER(pmapi.pmResult), POINTER(c_int), POINTER(pmapi.pmAtomValue), c_int, c_int, c_int]

def pcpfastExtractValues(result_p, vsetidx, vlistidx, dtype):
    """ quicker implementation of pmExtractValue than the default provided with the pcp python bindings
        this version saves converting the C indexes to python and back again
    """

    inst = c_int()
    outAtom = pmapi.pmAtomValue()
    status = LIBPCPFAST.pcpfastExtractValues(result_p, byref(inst), byref(outAtom), vsetidx, vlistidx, dtype)
    if status < 0:
        raise pmapi.pmErr(status)

    if dtype == c_api.PM_TYPE_STRING:
        # Get pointer to C string
        c_str = c_char_p()
        memmove(byref(c_str), addressof(outAtom) + pmapi.pmAtomValue.cp.offset, sizeof(c_char_p))
        # Convert to a python string and have result point to it
        outAtom.cp = outAtom.cp
        # Free the C string
        LIBC.free(c_str)

    return outAtom.dref(dtype), inst.value
