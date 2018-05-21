"""puffypcp is a direct interface to pcp c library"""

from pcp import pmapi
from libc.stdlib cimport free, malloc
from libc.stdint cimport uintptr_t
from cpython cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_SIMPLE
import cpmapi as c_pmapi
import numpy
from ctypes import c_uint

cimport pcp
cimport numpy

# Memory pool
# Dealloc's references when garbage collected
# Inspired by spacey-io's cymem package (https://github.com/spacy-io/cymem)
cdef class Pool:
    cdef addresses

    def __cinit__(self):
        self.addresses = []
 
    def __dealloc__(self):
        cdef uintptr_t addr
        if len(self.addresses) > 0:
            for addr in self.addresses:
                if addr != 0:
                    free(<void*>addr)

    cdef void add(self, void* p) except *:
        if p == NULL:
            raise MemoryError("Invalid pointer")
        self.addresses.append(<uintptr_t>p)

cdef object topyobj(pcp.pmAtomValue atom, int dtype):
    if dtype == pcp.PM_TYPE_STRING:
        ret = str(atom.cp)
        free(atom.cp)
        return ret
    elif dtype == pcp.PM_TYPE_32:
        return long(atom.l)
    elif dtype == pcp.PM_TYPE_U32:
        return long(atom.ul)
    elif dtype == pcp.PM_TYPE_64:
        return long(atom.ll)
    elif dtype == pcp.PM_TYPE_U64:
        return long(atom.ull)
    elif dtype == pcp.PM_TYPE_DOUBLE:
        return long(atom.d)
    else: # Don't know how to handle data type
        return None

cdef object strinnerloop(int numval, pcp.pmResult* res, int i):
    cdef Py_ssize_t j
    cdef pcp.pmAtomValue atom
    cdef int status
    tmp_data = list()
    for j in xrange(numval):
        status = pcp.pmExtractValue(res.vset[i].valfmt, &res.vset[i].vlist[j], pcp.PM_TYPE_STRING, &atom, pcp.PM_TYPE_STRING)
        if status < 0:
            raise pmapi.pmErr(status)
        tmp_data.append(str(atom.cp))
        free(atom.cp)
    return numpy.array(tmp_data)

cdef numpy.ndarray[double, ndim=1, mode="c"] int32innerloop(int numval, pcp.pmResult* res, int i):
    cdef Py_ssize_t j
    cdef pcp.pmAtomValue atom
    cdef int status
    cdef numpy.ndarray[double, ndim=1, mode="c"] tmp_data = numpy.empty(numval, dtype=numpy.float64)
    tmp_data = tmp_data # To update cython reference
    for j in xrange(numval):
        status = pcp.pmExtractValue(res.vset[i].valfmt, &res.vset[i].vlist[j], pcp.PM_TYPE_32, &atom, pcp.PM_TYPE_32)
        if status < 0:
            raise pmapi.pmErr(status)
        tmp_data[j] = <double>atom.l
    return tmp_data

cdef numpy.ndarray[double, ndim=1, mode="c"] uint32innerloop(int numval, pcp.pmResult* res, int i):
    cdef Py_ssize_t j
    cdef int status
    cdef pcp.pmAtomValue atom
    cdef numpy.ndarray[double, ndim=1, mode="c"] tmp_data = numpy.empty(numval, dtype=numpy.float64)
    tmp_data = tmp_data
    for j in xrange(numval):
        inst = res.vset[i].vlist[j].inst
        status = pcp.pmExtractValue(res.vset[i].valfmt, &res.vset[i].vlist[j], pcp.PM_TYPE_U32, &atom, pcp.PM_TYPE_U32)
        if status < 0:
            raise pmapi.pmErr(status)
        tmp_data[j] = <double>atom.ul
    return tmp_data

cdef numpy.ndarray[double, ndim=1, mode="c"] int64innerloop(int numval, pcp.pmResult* res, int i):
    cdef Py_ssize_t j
    cdef pcp.pmAtomValue atom
    cdef int status
    cdef numpy.ndarray[double, ndim=1, mode="c"] tmp_data = numpy.empty(numval, dtype=numpy.float64)
    tmp_data = tmp_data
    for j in xrange(numval):
        inst = res.vset[i].vlist[j].inst
        status = pcp.pmExtractValue(res.vset[i].valfmt, &res.vset[i].vlist[j], pcp.PM_TYPE_64, &atom, pcp.PM_TYPE_64)
        if status < 0:
            raise pmapi.pmErr(status)
        tmp_data[j] = <double>atom.ll
    return tmp_data

cdef numpy.ndarray[double, ndim=1, mode="c"] uint64innerloop(int numval, pcp.pmResult* res, int i):
    cdef Py_ssize_t j
    cdef pcp.pmAtomValue atom
    cdef int status
    cdef numpy.ndarray[double, ndim=1, mode="c"] tmp_data = numpy.empty(numval, dtype=numpy.float64)
    tmp_data = tmp_data
    for j in xrange(numval):
        inst = res.vset[i].vlist[j].inst
        status = pcp.pmExtractValue(res.vset[i].valfmt, &res.vset[i].vlist[j], pcp.PM_TYPE_U64, &atom, pcp.PM_TYPE_U64)
        if status < 0:
            raise pmapi.pmErr(status)
        tmp_data[j] = <double>atom.ull
    return tmp_data

cdef numpy.ndarray[double, ndim=1, mode="c"] doubleinnerloop(int numval, pcp.pmResult* res, int i):
    cdef Py_ssize_t j
    cdef pcp.pmAtomValue atom
    cdef int status
    cdef numpy.ndarray[double, ndim=1, mode="c"] tmp_data = numpy.empty(numval, dtype=numpy.float64)
    cdef double* tmp_datap = &tmp_data[0]
    for j in xrange(numval):
        inst = res.vset[i].vlist[j].inst
        status = pcp.pmExtractValue(res.vset[i].valfmt, &res.vset[i].vlist[j], pcp.PM_TYPE_DOUBLE, &atom, pcp.PM_TYPE_DOUBLE)
        if status < 0:
            raise pmapi.pmErr(status)
        tmp_datap[j] = atom.d
    return tmp_data

# All numeric types return numpy.float64 (c double) arrays
# Functions are seperated based on type to handle any quirks from converting to double
cdef object extractValuesInnerLoop(Py_ssize_t numval, pcp.pmResult* res, int dtype, int i):
    """extracts values and wraps them in numpy arrays"""
    if dtype == pcp.PM_TYPE_STRING:
        return strinnerloop(numval, res, i)
    elif dtype == pcp.PM_TYPE_32:
        return int32innerloop(numval, res, i)
    elif dtype == pcp.PM_TYPE_U32:
        return uint32innerloop(numval, res, i)
    elif dtype == pcp.PM_TYPE_64:
        return int64innerloop(numval, res, i)
    elif dtype == pcp.PM_TYPE_U64:
        return uint64innerloop(numval, res, i)
    elif dtype == pcp.PM_TYPE_DOUBLE:
        return doubleinnerloop(numval, res, i)
    else: # Don't know how to handle data type
        return []

cdef char* lookup(int val, int len, int* instlist, char** namelist):
    cdef int i
    for i in xrange(len):
        if instlist[i] == val:
            return namelist[i]
    return NULL

def extractValues(context, result, py_metric_id_array, mtypes, logerr):
    """
    returns data, description
    data is in format:  list (entry for each pmid)
                           |--> numpy array for pmid 0
                                   |--> inst 0 value
                                   |--> inst 1 value
                                   ...
                           |--> numpy array for pmid 1
                                   ...

    description in format:   list (entry for each pmid)
                                |--> tuple (for pmid 0)
                                        |--> numpy array (entry for each inst)
                                                |--> inst 0 id
                                                |--> inst 1 id
                                                ...
                                        |--> list (entry for each inst)
                                                |--> inst 0 name
                                                |--> inst 1 name
                                                ...
                                ...
    """
    data = []
    description = []
    mem = Pool()

    cdef Py_buffer buf
    PyObject_GetBuffer(result.contents, &buf, PyBUF_SIMPLE)
    cdef pcp.pmResult* res = <pcp.pmResult*>buf.buf
    cdef int ninstances
    cdef int numpmid = res.numpmid
    cdef Py_ssize_t i, j
    cdef int ctx = context._ctx
    cdef int status
    cdef int* ivals
    cdef char** inames
    cdef char* name
    cdef pcp.pmDesc metric_desc
    cdef pcp.pmAtomValue atom
    cdef int dtype
    cdef int allempty = 1

    if numpmid < 0:
        logerr("negative number of pmid's")
        PyBuffer_Release(&buf)
        return None, None

    cdef pcp.pmID* metric_id_array = <pcp.pmID*>malloc(numpmid * sizeof(pcp.pmID))
    mem.add(metric_id_array)
    for i in xrange(numpmid):
        metric_id_array[i] = py_metric_id_array[i] # Implicit py object to c data type conversion
    pcp.pmUseContext(ctx)

    for i in xrange(numpmid):
        ninstances = res.vset[i].numval
        ninstances = ninstances
        if ninstances < 0:
            logerr("negative number of instances")
            PyBuffer_Release(&buf)
            return None, None
        # No instances, but there needs to be placeholders
        elif ninstances == 0:
            data.append(numpy.empty(0, dtype=numpy.float64))
            description.append([numpy.empty(0, dtype=numpy.int64), []])
        else:
            dtype = mtypes[i]
            tmp_names = []
            tmp_idx = numpy.empty(ninstances, dtype=int)

            # extractValueInnerLoop does own looping
            data.append(extractValuesInnerLoop(ninstances, res, dtype, i))
            if len(data[i]) > 0:
                allempty = 0
            elif data[i] == []:
                logerr("unkown data type on extraction")

            status = pcp.pmLookupDesc(metric_id_array[i], &metric_desc)
            if status < 0:
                PyBuffer_Release(&buf)
                return None, None
            status = pcp.pmGetInDom(metric_desc.indom, &ivals, &inames)
            if status < 0:
                if len(data[i]) != 0: # Found data, so insert placeholder description
                    description.append([numpy.empty(0, dtype=numpy.int64), []])
                else:
                    PyBuffer_Release(&buf)
                    return None, None
            elif ninstances > status: # Missing a few indoms - try again
                mem.add(ivals)
                mem.add(inames)
                PyBuffer_Release(&buf)
                return True, True
            else:
                mem.add(ivals)
                mem.add(inames)
                for j in xrange(ninstances):
                    if res.vset[i].vlist[j].inst == 4294967295:
                        logerr("inst is -1")
                        continue
                    # TODO - find way to just look for one name not generate list then find it in list
                    name = lookup(res.vset[i].vlist[j].inst, status, ivals, inames)
                    if name == NULL:
                        logerr("instance is not pcp archive")
                        continue # Possibly add logging here
                    tmp_names.append(name)
                    tmp_idx[j] = res.vset[i].vlist[j].inst
                        
                description.append([tmp_idx, tmp_names])


    PyBuffer_Release(&buf)
    if allempty:
        return None, None

    return data, description

def extractpreprocValues(context, result, py_metric_id_array, mtypes):
    """
    populate and return data, description from pcp archive for preproc's
    data is in format: list (entry for each pmid)
                        |--> list (entry for each instance)
                                |--> list (pmid 0, instance 0)
                                        |--> value
                                        |--> instance
                                |--> list (pmid0, instance 1)
                                        ...
                                ...
                        ...

    description in format: list (entry for each metric id)
                            |--> dict (entry for each instance in metric_id 0, not guarenteed in order)
                                    |--> instance 0 id => instance 0 name
                                    |--> instance 1 id => instance 1 name
                                    ...
                            |--> dict (metric id 1)
                                    ...
                            ...
    """
    data = []
    description = []
    mem = Pool()

    cdef Py_buffer buf
    PyObject_GetBuffer(result.contents, &buf, PyBUF_SIMPLE)
    cdef pcp.pmResult* res = <pcp.pmResult*> buf.buf
    cdef int mid_len = len(py_metric_id_array)
    cdef int numpmid = res.numpmid
    cdef int ninstances
    cdef Py_ssize_t i, j
    cdef int ctx = context._ctx
    cdef int status
    cdef int* ivals
    cdef char** inames
    cdef pcp.pmDesc metric_desc
    cdef pcp.pmAtomValue atom
    cdef int dtype

    if mid_len < 0:
        PyBuffer_Release(&buf)
        return None, None
    cdef pcp.pmID* metric_id_array = <pcp.pmID*>malloc(mid_len * sizeof(pcp.pmID))
    mem.add(metric_id_array)
 
    for i in xrange(mid_len):
        metric_id_array[i] = py_metric_id_array[i] # Implicit py object to c data type conversion
    pcp.pmUseContext(ctx)
    
    # Initialize description
    for i in xrange(mid_len):
        pcp.pmLookupDesc(metric_id_array[i], &metric_desc)
        if 4294967295 == metric_desc.indom: # Missing indom - skip
            continue
        status = pcp.pmGetInDom(metric_desc.indom, &ivals, &inames)
        if status <= 0:
            description.append({})
        else:
            mem.add(ivals)
            mem.add(inames)
            tmp_dict = dict()
            for j in xrange(status):
                tmp_dict[ivals[j]] = inames[j]
            description.append(tmp_dict)

    # Initialize data
    for i in xrange(numpmid):
        ninstances = res.vset[i].numval
        ninstances = ninstances
        pcp.pmLookupDesc(metric_id_array[i], &metric_desc)

        tmp_data = []
        dtype = mtypes[i]

        for j in xrange(ninstances):
            status = pcp.pmExtractValue(res.vset[i].valfmt, &res.vset[i].vlist[j], dtype, &atom, dtype)
            if status < 0:
                tmp_data.append([])
            else:
                tmp_data.append([topyobj(atom, dtype), res.vset[i].vlist[j].inst])
        data.append(tmp_data)

    PyBuffer_Release(&buf)
    return numpy.array(data), description

def loadrequiredmetrics(context, requiredMetrics):
    """ required metrics are those that must be present for the analytic to be run """
    mem = Pool()
    cdef int num_met = len(requiredMetrics)
    cdef int ctx = context._ctx
    pcp.pmUseContext(ctx)
    cdef Py_ssize_t i
    cdef int status
    cdef char** nameofmetrics = <char**>malloc(num_met * sizeof(char*))
    mem.add(nameofmetrics)
    for i in xrange(num_met):
        nameofmetrics[i] = requiredMetrics[i]
    
    cdef pcp.pmID* required = <pcp.pmID*>malloc(num_met * sizeof(pcp.pmID*))
    mem.add(required)
    status = pcp.pmLookupName(num_met, nameofmetrics, required)
    if status < 0:
        return []
    if status != num_met:
        return []
 
    ret = []
    for i in xrange(num_met):
        ret.append(required[i])

    return ret

def getmetricstofetch(context, analytic):
    """ returns the c_type data structure with the list of metrics requested
        for the analytic """

    metriclist = []
    metricnames = []

    for derived in analytic.derivedMetrics:
        context.pmRegisterDerived(derived['name'], derived['formula'])
        required = context.pmLookupName(derived['name'])
        metriclist.append(required[0])
        metricnames.append(derived['name'])

    if len(analytic.requiredMetrics) > 0:
        metricOk = False
        if isinstance(analytic.requiredMetrics[0], basestring):
            r = loadrequiredmetrics(context, analytic.requiredMetrics)
            if len(r) > 0:
                metriclist += r
                metricnames.extend(analytic.requiredMetrics)
                metricOk = True
        else:
            for reqarray in analytic.requiredMetrics:
                r = loadrequiredmetrics(context, reqarray)
                if len(r) > 0:
                    metriclist += r
                    metricnames.extend(reqarray)
                    metricOk = True
                    break

        if not metricOk:
            return [], []

    for optional in analytic.optionalMetrics:
        try:
            opt = context.pmLookupName(optional)
            metriclist.append(opt[0])
            metricnames.append(optional)
        except pmapi.pmErr as e:
            if e.args[0] == c_pmapi.PM_ERR_NAME or e.args[0] == c_pmapi.PM_ERR_NONLEAF:
                # Optional metrics are allowed to not exist
                pass
            else:
                raise e


    metricarray = (c_uint * len(metriclist))()
    cdef Py_ssize_t i
    for i in xrange(0, len(metriclist)):
        metricarray[i] = metriclist[i]

    return metricarray, metricnames

def getmetrictypes(context, py_metric_ids):
    """ returns a list with the datatype of the provided array of metric ids """
    mem = Pool()

    cdef int num_mid = len(py_metric_ids)
    cdef Py_ssize_t i
    cdef pcp.pmID* metric_ids = <pcp.pmID*>malloc(num_mid * sizeof(pcp.pmID))
    mem.add(metric_ids)
    for i in xrange(num_mid):
        metric_ids[i] = py_metric_ids[i]
    cdef int ctx = context._ctx
    pcp.pmUseContext(ctx)
    cdef pcp.pmDesc d
    cdef int ty
    metrictypes = list()
    for i in xrange(num_mid):
        pcp.pmLookupDesc(metric_ids[i], &d)
        ty = d.type
        metrictypes.append(ty)

    return metrictypes

def pcptypetonumpy(pcptype):
    """ Convert pcp data types to numpy equivalents """
    if pcptype == pcp.PM_TYPE_STRING:
        return object
    return numpy.float

