// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replica.h:
 *   common interface to different replication protocols
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

#ifndef _COMMON_REPLICA_H_
#define _COMMON_REPLICA_H_


#include <folly/concurrency/ConcurrentHashMap.h>
#include "lib/configuration.h"
#include "common/log.h"
#include "common/request.pb.h"
#include "lib/transport.h"
#include "lib/viewstamp.h"
#include "lib/workertasks.h"

using folly::ConcurrentHashMap;

namespace specpaxos {

class Replica;

enum ReplicaStatus {
    STATUS_NORMAL,
    STATUS_VIEW_CHANGE,
    STATUS_RECOVERING,
    STATUS_GAP_COMMIT
};

class AppReplica
{
private:
    int opLength = 1;
    int totalBlocks = 0;
    std::vector<std::string> file;
    int currentAppendIndex = 0;


    void apply(string data) {
    	file.push_back(data); 
    	totalBlocks++;
    }

    string read(int blockNumber) {
    	if (blockNumber >= totalBlocks)
			return "NOTFOUND";
		return file[blockNumber];   
    }

    bool IsRead(string op) {
    	return !op.compare("r") || !op.compare("R");
    }

    bool IsAppend(string op) {
    	return !op.compare("a") || !op.compare("A");
    }

public:
    AppReplica() { };
    virtual ~AppReplica() { };
    // Invoke callback on the leader, with the option to replicate on success
    virtual void LeaderUpcall(opnum_t opnum, const string &str1, bool &replicate, string &str2) { replicate = true; str2 = str1; };
    // Invoke callback on all replicas
    virtual void ReplicaUpcall(opnum_t opnum, const string &str1, string &str2,
                               void *arg = nullptr, void *ret = nullptr) {
		size_t totalLen = str1.size();
		string op = str1.substr(0, opLength);
		string remaining = str1.substr(opLength, totalLen - opLength);
		assert(IsAppend(op)); // reads should not take this path in original VR
		apply(remaining);
		str2 = ""; // dummy result for sets
    };

    // Rollback callback on failed speculative operations
    virtual void RollbackUpcall(opnum_t current, opnum_t to, const std::map<opnum_t, string> &opMap) { };
    // Commit callback to commit speculative operations
    virtual void CommitUpcall(opnum_t) { };
    // Invoke call back for unreplicated operations run on only one replica
    virtual void UnloggedUpcall(const string &str1, string &str2) { 
        // read operation
    	size_t totalLen = str1.size();
    	string op = str1.substr(0, opLength);
		string remaining = str1.substr(opLength, totalLen - opLength);

        assert(IsRead(op));
    	str2 = read(std::stoi(remaining));
    };
};

class Replica : public TransportReceiver
{
public:
    Replica(const Configuration &config, int groupIdx, int replicaIdx,
            bool initialize, Transport *transport, AppReplica *app);
    virtual ~Replica();

protected:
    void LeaderUpcall(opnum_t opnum, const string &op, bool &replicate, string &res);
    void ReplicaUpcall(opnum_t opnum, const string &op, string &res,
                       void *arg = nullptr, void *ret = nullptr);
    template<class MSG> void Execute(opnum_t opnum,
                                     const Request & msg,
                                     MSG &reply,
                                     void *arg = nullptr,
                                     void *ret = nullptr);
    void Rollback(opnum_t current, opnum_t to, Log &log);
    void Commit(opnum_t op);
    void UnloggedUpcall(const string &op, string &res);
    template<class MSG> void ExecuteUnlogged(const UnloggedRequest & msg,
                                               MSG &reply);

protected:
    Configuration configuration;
    int groupIdx;
    int replicaIdx;
    Transport *transport;
    AppReplica *app;
    ReplicaStatus status;
};

#include "replica-inl.h"

} // namespace specpaxos

#endif  /* _COMMON_REPLICA_H */
