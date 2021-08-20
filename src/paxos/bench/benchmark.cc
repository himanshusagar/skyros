// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * benchmark.cpp:
 *   simple replication benchmark client
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "bench/benchmark.h"
#include "common/client.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "lib/timeval.h"

#include <sys/time.h>
#include <string>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>       
#include <netinet/in.h>
#include <arpa/inet.h>
#include <random>

#define OPCODE_SIZE 1
#define KEY_SIZE 24
#define VAL_SIZE 10

#define PORT 8080
#define SA struct sockaddr
std::random_device rd;
std::mt19937 e2(rd());
int sockfd, connfd;
int usingServer = 0;

namespace specpaxos {

DEFINE_LATENCY(op);

BenchmarkClient::BenchmarkClient(Client &client, Transport &transport,
                                 int numRequests, uint64_t delay,
                                 int warmupSec,
				 int tputInterval,
                 string traceFile,
                 int experimentDuration,
                 string latencyFilename)
    : tputInterval(tputInterval), client(client),
    transport(transport), numRequests(numRequests),
    delay(delay), warmupSec(warmupSec), traceFile(traceFile),
    experimentDuration(experimentDuration), latencyFilename(latencyFilename)
{
    if (delay != 0) {
        Notice("Delay between requests: %ld ms", delay);
    }
    started = false;
    done = false;
    cooldownDone = false;
    _Latency_Init(&latency, "op");
    latencies.reserve(numRequests);
    opcodes.reserve(numRequests);

    Notice("Using tracefile: %s", traceFile.c_str());
    if(traceFile.compare("nullfile")) {
        string line;
        std::ifstream tfile;
        tfile.open(traceFile);
        assert(tfile.is_open());

        while(getline(tfile, line)) {
            string op = line.substr(0, OPCODE_SIZE);
            string key = line.substr(2, line.size() - OPCODE_SIZE - 1);
            operations.push_back(std::make_pair(op, key));
            // Notice("%s%s", op.c_str(), key.c_str());
        }

        Notice("Loaded %lu operations (numrequests = %d) from tracefile: %s",
         operations.size(), numRequests, traceFile.c_str());
    } else {
        usingServer = 1;
        Notice("Nullfile. Will talk to server for requests...");
        struct sockaddr_in servaddr;
  
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            printf("socket creation failed...\n");
            exit(0);
        }
        bzero(&servaddr, sizeof(servaddr));
      
        servaddr.sin_family = AF_INET;
        servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
        servaddr.sin_port = htons(PORT);
      
        if (connect(sockfd, (SA*)&servaddr, sizeof(servaddr)) != 0) {
            printf("connection with the server failed...\n");
            exit(0);
        }
    }
}

char get_op() {
    std::uniform_real_distribution<float> dist(0, 1);
    if (dist(e2) < 0.5) 
        return 'r';    
    return 'u';
}

void
BenchmarkClient::Start()
{
    n = 0;
    transport.Timer(warmupSec * 1000,
                    std::bind(&BenchmarkClient::WarmupDone,
                               this));

    if (tputInterval > 0) {
	msSinceStart = 0;
	opLastInterval = n;
	transport.Timer(tputInterval, std::bind(&BenchmarkClient::TimeInterval,
						this));
    }
    expStartTime = std::chrono::high_resolution_clock::now();
    SendNext();
}

void
BenchmarkClient::TimeInterval()
{
    if (done) {
	return;
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    msSinceStart += tputInterval;
    Notice("Completed %d requests at %lu ms", n-opLastInterval, (((tv.tv_sec*1000000+tv.tv_usec)/1000)/10)*10);
    opLastInterval = n;
    transport.Timer(tputInterval, std::bind(&BenchmarkClient::TimeInterval,
					    this));
}

void
BenchmarkClient::WarmupDone()
{
    started = true;
    Notice("Completed warmup period of %d seconds with %d requests",
           warmupSec, n);
    gettimeofday(&startTime, NULL);
    n = 0;
}

void
BenchmarkClient::CooldownDone()
{

    char buf[1024];
    cooldownDone = true;
    Notice("Finished cooldown period.");
    std::vector<uint64_t> sorted = latencies;
    std::sort(sorted.begin(), sorted.end());

    uint64_t ns = sorted[sorted.size()/2];
    LatencyFmtNS(ns, buf);
    Notice("Median latency is %ld ns (%s)", ns, buf);

    ns = 0;
    for (auto latency : sorted) {
        ns += latency;
    }
    ns = ns / sorted.size();
    LatencyFmtNS(ns, buf);
    Notice("Average latency is %ld ns (%s)", ns, buf);

    ns = sorted[sorted.size()*90/100];
    LatencyFmtNS(ns, buf);
    Notice("90th percentile latency is %ld ns (%s)", ns, buf);

    ns = sorted[sorted.size()*95/100];
    LatencyFmtNS(ns, buf);
    Notice("95th percentile latency is %ld ns (%s)", ns, buf);

    ns = sorted[sorted.size()*99/100];
    LatencyFmtNS(ns, buf);
    Notice("99th percentile latency is %ld ns (%s)", ns, buf);
}

void
BenchmarkClient::SendNext()
{
    if(n >= numRequests){
        return;
    }

    std::string msg = "";
	bool isNonNilext;

    if(usingServer) {
        char buff;
        int resp;
        buff = get_op();
        assert(write(sockfd, &buff, sizeof(char)) == sizeof(char));
        bzero(&resp, sizeof(int));
        assert(read(sockfd, &resp, sizeof(int)) == sizeof(int));
        // Notice("Op: %c key from server : %d", buff, resp);    
        msg += buff;
        msg += std::to_string(resp);
	    isNonNilext = msg.c_str()[0] == 'e' || msg.c_str()[0] == 'E';
        opcodes.push_back(buff);
    } else {
        std::ostringstream msgstream;
        msgstream << operations[n].first << operations[n].second;
        msg = msgstream.str();
	    isNonNilext = msg.c_str()[0] == 'e' || msg.c_str()[0] == 'E';
        opcodes.push_back(operations[n].first[0]);
    }

    Latency_Start(&latency);
    client.Invoke(msg, std::bind(&BenchmarkClient::OnReply,
                                       this,
                                       std::placeholders::_1,
                                       std::placeholders::_2));
}

void
BenchmarkClient::OnReply(const string &request, const string &reply)
{
    if (cooldownDone) {
        return;
    }

    n++;
    if ((started) && (!done) && (n != 0)) {
    	uint64_t ns = Latency_End(&latency);
    	latencies.push_back(ns);
    	if (n >= numRequests) {
    	    Finish();
    	}

        auto current_time = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(current_time 
            - expStartTime).count() >= experimentDuration) {
			Notice("Experiment duration elasped. Exiting.");
            Finish();
        }
    }
    
    if (delay == 0) {
       SendNext();
    } else {
        uint64_t rdelay = rand() % delay*2;
        transport.Timer(rdelay,
                        std::bind(&BenchmarkClient::SendNext, this));
    }
}

void
BenchmarkClient::Finish()
{
    gettimeofday(&endTime, NULL);

    struct timeval diff = timeval_sub(endTime, startTime);

    Notice("Completed %d requests in " FMT_TIMEVAL_DIFF " seconds",
           n, VA_TIMEVAL_DIFF(diff));
    done = true;

    /*if(usingServer) {
        char buff = 'e';
        assert(write(sockfd, &buff, sizeof(char)) == sizeof(char));
    }*/

    transport.Timer(warmupSec * 1000,
                    std::bind(&BenchmarkClient::CooldownDone,
                              this));


    if (latencyFilename.size() > 0) {
        Latency_FlushTo(latencyFilename.c_str());
    }
}


} // namespace specpaxos
