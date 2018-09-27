from pcp import pmapi # Python bindings

cdef extern from "sys/time.h":
    ctypedef struct timeval:
        pass

cdef extern from "pcp/pmapi.h":
    # Errors
    int PM_ERR_GENERIC   =  "PM_ERR_GENERIC"
    int PM_ERR_PMID      =  "PM_ERR_PMID"
    int PM_ERR_INDOM     =  "PM_ERR_INDOM"
    int PM_ERR_INST      =  "PM_ERR_INST"
    int PM_ERR_PMID_LOG  =  "PM_ERR_PMID_LOG"
    int PM_ERR_INDOM_LOG =  "PM_ERR_INDOM_LOG"
    int PM_ERR_INST_LOG  =  "PM_ERR_INST_LOG"
    int PM_ERR_NAME      =  "PM_ERR_NAME"
    int PM_ERR_SIGN      =  "PM_ERR_SIGN"

    # pmDesc.type -- data type of metric values 
    int PM_TYPE_NOSUPPORT        = "PM_TYPE_NOSUPPORT"
    int PM_TYPE_32               = "PM_TYPE_32"
    int PM_TYPE_U32              = "PM_TYPE_U32"
    int PM_TYPE_64               = "PM_TYPE_64"
    int PM_TYPE_U64              = "PM_TYPE_U64"
    int PM_TYPE_FLOAT            = "PM_TYPE_FLOAT"
    int PM_TYPE_DOUBLE           = "PM_TYPE_DOUBLE"
    int PM_TYPE_STRING           = "PM_TYPE_STRING"
    int PM_TYPE_AGGREGATE        = "PM_TYPE_AGGREGATE"
    int PM_TYPE_AGGREGATE_STATIC = "PM_TYPE_AGGREGATE_STATIC"
    int PM_TYPE_EVENT            = "PM_TYPE_EVENT"
    int PM_TYPE_HIGHRES_EVENT    = "PM_TYPE_HIGHRES_EVENT"
    int PM_TYPE_UNKNOWN          = "PM_TYPE_UNKNOWN"

    ctypedef struct pmUnits:
        pass
    ctypedef unsigned int pmID
    ctypedef unsigned int pmInDom
    ctypedef struct pmValueBlock:
        pass
    ctypedef union myvalue:
        pmValueBlock* pval
        int lval
    ctypedef struct pmValue: # Can't declare anonymous union
        int inst
        myvalue value
    ctypedef struct pmDesc:
        pmID pmid
        int type
        pmInDom indom
        int sem
        pmUnits units
    ctypedef struct pmValueSet:
        pmID pmid
        int numval
        int valfmt
        pmValue vlist[1]
    ctypedef struct pmResult:
        timeval timestamp
        int numpmid
        pmValueSet *vset[1]
    ctypedef union pmAtomValue:
        # TODO use <inttypes.h> types instead of simple long etc.
        char* cp
        long l
        unsigned long ul
        long long ll
        unsigned long long ull
        float f
        double d

    pmInDom PM_INDOM_NULL

    int pmLookupName(int, char **, pmID *)
    int pmLookupDesc(pmID, pmDesc *)
    int pmLookupInDom(pmInDom, const char *)
    int pmLookupInDomArchive(pmInDom, const char *)
    int pmNameInDom(pmInDom, int, char **)
    int pmNameInDomArchive(pmInDom, int, char **)
    int pmUseContext(int)
    int pmGetInDom(pmInDom, int **, char ***)
    int pmGetInDomArchive(pmInDom, int **, char ***)
    int pmExtractValue(int, const pmValue *, int, pmAtomValue *, int)
    char *pmErrStr(int)
