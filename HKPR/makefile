CCOMPILE=mpic++
HADOOP_HOME=/opt/hadoop/
JAVA_HOME=/usr/local/jdk/jdk-11.0.1/
System_HOME=../system/
SZ_HOME=../SZ3/include
CPPFLAGS= -I$(HADOOP_HOME)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I$(System_HOME) -I$(SZ_HOME) -Wno-deprecated -O2
LIB := -L$(HADOOP_HOME)/lib/native -L/usr/local/lib64/ -L../SZ3/lib64
LDFLAGS = -lhdfs -lstdc++ -lzstd

all: run

run: run.cpp
	$(CCOMPILE) run.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o run

clean:
	-rm run