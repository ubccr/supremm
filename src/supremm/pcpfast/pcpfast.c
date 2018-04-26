#include <pcp/pmapi.h>

int pcpfastExtractValues(pmResult *rp, int *inst, pmAtomValue *atom, int vsetidx, int vlistidx, int type)
{
    int status;

    *inst = rp->vset[vsetidx]->vlist[vlistidx].inst;
    status = pmExtractValue(rp->vset[vsetidx]->valfmt, &rp->vset[vsetidx]->vlist[vlistidx], type, atom, type);

    return status;
}
