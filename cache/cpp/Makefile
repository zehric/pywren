CXX=g++
CXXFLAGS=-g -O3 -std=c++11 -laws-cpp-sdk-core -laws-cpp-sdk-s3 -lpthread
SOEXT ?= so

# all: fastio.$(SOEXT) cache_test
all: cache_test

# fastio.$(SOEXT): fastio.cpp
# 	 $(CXX) -fPIC -c -o fastio_shared.o $< $(CXXFLAGS)
# 	 $(CXX)  -dynamiclib -shared -o $@ fastio_shared.o $(CXXFLAGS)
# 	 aws s3 cp fastio.$(SOEXT) s3://zehric-pywren-149/

cache_test: cache.cpp
	 $(CXX) -c -o cache_test.o $< $(CXXFLAGS)
	 $(CXX) -o $@ cache_test.o $(CXXFLAGS) -Wl,-rpath,/home/ec2-user/aws-sdk-cpp-build/.deps/install/lib

clean:
	rm -rf *.o
	rm -f cache_test.o
	rm -f cache_test
