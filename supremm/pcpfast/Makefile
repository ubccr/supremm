SONAME=libpcpfast.so
SOVERSION=1
OBJS=pcpfast.o
LDLIBS= -lc -lpcp
CFLAGS=-fPIC -Wall -O2

$(SONAME): $(OBJS)
	$(CC) $(CFLAGS) -shared -Wl,-soname,$(SONAME).$(SOVERSION) -o $(SONAME) $(OBJS) $(LDLIBS)

.PHONY: clean

clean:
	$(RM) $(SONAME) $(OBJS)
