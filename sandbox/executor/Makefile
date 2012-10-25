CXXFLAGS =	-O2 -g -Wall -fmessage-length=0

OBJS =		drill_executor.o

LIBS =

TARGET =	drill_executor

$(TARGET):	$(OBJS)
	$(CXX) -o $(TARGET) $(OBJS) $(LIBS)

all:	$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
