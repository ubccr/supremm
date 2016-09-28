cdef extern from *:
    ctypedef char const_char "const char"

cdef extern from 'dlfcn.h' nogil:
    void* dlopen(const_char *filename, int flag)
    char *dlerror()
    void *dlsym(void *handle, const_char *symbol)
    int dlclose(void *handle)

    unsigned int RTLD_LAZY
    unsigned int RTLD_NOW
    unsigned int RTLD_GLOBAL
    unsigned int RTLD_LOCAL
    unsigned int RTLD_NODELETE
    unsigned int RTLD_NOLOAD
    unsigned int RTLD_DEEPBIND

    unsigned int RTLD_DEFAULT
    long unsigned int RTLD_NEXT
