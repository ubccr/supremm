cimport dlsymdefs

from libc.stdlib cimport malloc, free
from cpython.string cimport PyString_AsString

cdef char ** to_cstring_array(list_str):
    cdef char **ret = <char **>malloc(len(list_str) * sizeof(char *))
    for i in xrange(len(list_str)):
        ret[i] = PyString_AsString(list_str[i])
    return ret

def pypmlogextract(args):

    cdef void *handle = dlsymdefs.dlopen("libpcp_pmlogextract.so.1",dlsymdefs.RTLD_LAZY)
    
    if handle == NULL:
        print dlsymdefs.dlerror()
        return 1
    
    cdef void *mainFunc = dlsymdefs.dlsym(handle, "mainFunc")
    if mainFunc == NULL:
        print dlsymdefs.dlerror()
        return 1
    
    args.insert(0, "pypmlogextract")

    cdef char **myargv = to_cstring_array(args)
    cdef int myargc = len(args)
    
    cdef int retval = (<int (*)(int, char**)> mainFunc)(myargc, myargv)

    # PyString_AsString returns a ref to an internal buffer that shouldn't be freed per the docs
    # Just free our malloc
    free(myargv)
    dlsymdefs.dlclose(handle)

    return retval
