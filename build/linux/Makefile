#
# Makefile for threadpool
#

PROJECT_NAME=threadpool

INCONCERT_DEVEL   ?=../../../..
INCONCERT_SOURCES ?=$(INCONCERT_DEVEL)/sources
INCONCERT_BIN     ?=$(INCONCERT_DEVEL)/bin
INCONCERT_LIB     ?=$(INCONCERT_DEVEL)/lib
PACKAGES          ?=$(INCONCERT_DEVEL)/packages
INCLUDE_DIR       ?=/usr/local/include
LIB_DIR           ?=/usr/local/lib

OUTPUT_DIR  ?= $(INCONCERT_BIN)

BOOST_VERSION ?=1_44
BOOST_DIR     ?=$(INCLUDE_DIR)/boost-$(BOOST_VERSION)
BOOST_LIBS    ?=$(LIB_DIR)

INCONCERT_COMPILER   ?= g++
INCONCERT_COMPILER_C ?= gcc
GCC_VERSION          ?= $(shell expr substr "`$(INCONCERT_COMPILER) -dumpversion | tr -dc '[0-9]'`" 1 2)

# let the user override boost libs if he wants to
BOOST_THREAD_LIB       ?=boost_thread-gcc$(GCC_VERSION)-mt-$(BOOST_VERSION)
BOOST_THREAD_LIBD      ?=boost_thread-gcc$(GCC_VERSION)-mt-d-$(BOOST_VERSION)

CPPC   =$(INCONCERT_COMPILER)
CC     =$(INCONCERT_COMPILER_C)
LINKER =$(INCONCERT_COMPILER)
ARCHIVER = ar

INCLUDES = -I$(BOOST_DIR)

CFLAGS  :=$(CFLAGS) -fPIC -c -Wall -Wextra -pedantic \
	-fno-strict-aliasing \
	-DGCC_VERSION=$(GCC_VERSION)

ifeq (,$(findstring -Wno-error,$(CFLAGS)))
	CFLAGS :=$(CFLAGS) -Werror
endif

LDFLAGS :=$(LDFLAGS) -Wall -fPIC -shared -g

ifeq ($(origin OBJS_ROOT), undefined)
	OBJS_ROOT :=.
else
	OBJS_ROOT := $(OBJS_ROOT)/$(PROJECT_NAME)
endif

Target ?= Release

ROOT_DIR =../..
SRC_DIR  =$(ROOT_DIR)/src
OBJS_DIR =$(OBJS_ROOT)/$(Target)

ifeq (Debug, $(findstring Debug,$(Target)))
	LIB_NAME          =$(PROJECT_NAME)_d
	CFLAGS           :=$(CFLAGS) -D_DEBUG -g3
	BOOST_THREAD_LIB  =$(BOOST_THREAD_LIBD)
else
	LIB_NAME  =$(PROJECT_NAME)
	CFLAGS   :=$(CFLAGS) -O2 -g
endif

OUTPUT_LIB   =lib$(LIB_NAME).so
OUTPUT_FILE  =$(OUTPUT_DIR)/$(OUTPUT_LIB)
LDFLAGS     :=$(LDFLAGS) -Wl,-soname,$(OUTPUT_LIB)
LIBS         =-L$(BOOST_LIBS) -l$(BOOST_THREAD_LIB)
AFLAGS = rcs

OBJS = $(OBJS_DIR)/pool.o

COMPILE =$(CPPC) $(CFLAGS) "$<" -o "$(OBJS_DIR)/$(*F).o" $(INCLUDES)
LINK    =$(LINKER) $(LDFLAGS) -o "$(OUTPUT_FILE)" $(OBJS) $(LIBS)

# default target
build: $(OUTPUT_FILE)
$(OUTPUT_FILE): $(OBJS_DIR) $(OUTPUT_DIR) $(OBJS)
	$(LINK)

build_static: $(INCONCERT_LIB)/lib$(LIB_NAME).a
$(INCONCERT_LIB)/lib$(LIB_NAME).a: $(OBJS_DIR) $(OUTPUT_DIR) $(OBJS)
	$(ARCHIVER) $(AFLAGS) "$(INCONCERT_LIB)/lib$(LIB_NAME).a" $(OBJS)

$(OUTPUT_DIR):
	mkdir -p $(OUTPUT_DIR)

$(OBJS_DIR):
	mkdir -p $(OBJS_DIR)

# Pattern rules:
$(OBJS_DIR)/%.o: $(SRC_DIR)/%.cpp
	$(COMPILE)

$(OBJS_DIR)/pool.o: $(ROOT_DIR)/pool.h $(SRC_DIR)/pool.cpp

.PHONY: clean
clean:
	rm -f $(OBJS_DIR)/*.o
	rm -f $(OUTPUT_FILE)
	rm -f $(OUTPUT_DIR)/threadpool.bin

test_objs: $(OBJS_DIR)/tests.o $(ROOT_DIR)/pool.h
$(OBJS_DIR)/tests.o: $(ROOT_DIR)/tests/tests.cpp
	$(COMPILE) -I$(INCONCERT_SOURCES)

tests: build $(OUTPUT_DIR)/threadpool.bin
$(OUTPUT_DIR)/threadpool.bin: test_objs
	$(LINKER) -O2 -g -o $(OUTPUT_DIR)/threadpool.bin $(OBJS_DIR)/tests.o $(LIBS) -L$(OUTPUT_DIR) -l$(LIB_NAME)
