OS_PLATFORM=$(shell uname -s)
ifneq ($(OS_PLATFORM), HP-UX)
MSWIN_PLATFORM=$(shell uname -o)
endif

MCC=g++

# 动态库
LIBFLAGS=

LIB_END=.so
BIN_END=
SYS_LIBS=-lrt

# 判断系统平台
ifeq ($(MSWIN_PLATFORM), Msys)
CXXFLAGS=-g
LIB_END=.dll
BIN_END=.exe
SYS_LIBS=-lkernel32 -luser32 -lwinspool -lws2_32
else ifeq ($(OS_PLATFORM), Linux)
CXXFLAGS=-pipe -g -fPIC -std=c++11
else ifeq ($(OS_PLATFORM), HP-UX)
CXXFLAGS=-fpic -g -D_XOPEN_SOURCE_EXTENDED -ftemplate-depth-64 -pthread -mlp64
else
CXXFLAGS=-g -fPIC
endif


# 设置包含目录，库目录，源代码目录
INC=-I/usr/include 
LIB=-L/usr/lib -L/usr/local/lib -L/usr/lib/hpux64 -L/usr/local/lib/hpux64 
SRC=.

# 请不要修改此项
OUTPUT=.

# 系统目录
SYSINC=-I/include -I/usr/include -I/usr/local/include 
SYSLIB=-L/lib -L/usr/lib -L/usr/local/lib 

INCS=

# 链接库
LIBS= -lpthread  -lstdc++fs -ldl

# 生成名
NAME=TDengineData2.6T3.0

# 预定义宏
MARCO=

# 需要安装的头文件
INSTALL_HEADERS=

# 源代码文件
SOURCE= $(SRC)/main.cpp \
	$(SRC)/taos.cpp \



ifeq ($(LIBFLAGS), -shared)
OUTPUT_NAME=lib$(NAME)$(LIB_END)
else
OUTPUT_NAME=$(NAME)$(BIN_END)
endif

$(OUTPUT_NAME) : $(SOURCE)
	$(MCC) $(CXXFLAGS) $(LIBFLAGS) $(MARCO) $(INC) $(SYSINC) $(INCS) $(SOURCE) -o $(OUTPUT)/$(OUTPUT_NAME) $(LIB) $(SYSLIB) $(SYS_LIBS) $(LIBS)

clean:
	@rm -f *.o
	@rm -f $(OUTPUT)/$(OUTPUT_NAME)

# 安装设置
install:
	@if [ ! "$(LIBFLAGS)" ] ; then \
	cp $(OUTPUT)/$(OUTPUT_NAME) $(INTSALL_TOP)/bin; \
	else \
	cp $(OUTPUT)/$(OUTPUT_NAME) $(INTSALL_TOP)/lib; \
	fi
	@if [ ! "$(INSTALL_HEADERS)" ] ; then \
	echo "no install headers."; \
	else \
	cp $(INSTALL_HEADERS) $(INTSALL_TOP)/include; \
	fi



