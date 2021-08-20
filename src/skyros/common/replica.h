// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replica.h:
 *   common interface to different replication protocols
 * 
 * Copyright 2021 Aishwarya Ganesan and Ramnatthan Alagappan
 *
 * Significant changes made to the code to implement Skyros
 *
 * *************************************************************
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
#include "vr/vr-proto.pb.h"
#include <assert.h>

#include <queue>
#include <map>
#include <boost/lockfree/spsc_queue.hpp>

using folly::ConcurrentHashMap;

typedef std::pair<uint64_t, uint64_t> CXID;

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

    //the value in durability log is a tuple of position, request. key is clientid, clientrequestid
    ConcurrentHashMap<CXID, std::pair<uint64_t, specpaxos::vr::proto::RequestMessage>> durabilityLog;
    int durLogIndex = 0;

    void apply(string data) {
    	file.push_back(data); 
    	totalBlocks++;
    	// Notice("Appended a block. Total blocks: %d", totalBlocks);
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

    bool IsNonNilextWrite(string op) {
    	return !op.compare("e") || !op.compare("E");
    }

public:
	boost::lockfree::spsc_queue<specpaxos::vr::proto::RequestMessage> queue{10000000};
	ConcurrentHashMap<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    
    AppReplica(): durabilityLog(10*1000*1000){
    };

    virtual ~AppReplica() { };
	virtual void AddToQueue(specpaxos::vr::proto::RequestMessage msg) {
		while(!queue.push(msg));
	}

	virtual std::queue<specpaxos::vr::proto::RequestMessage> GetAndDeleteFromQueue() {
		std::queue<specpaxos::vr::proto::RequestMessage> tmp;
		specpaxos::vr::proto::RequestMessage tmp_msg;
        int to_read = queue.read_available();
        while (to_read) {
            if (queue.pop(tmp_msg)) {
                tmp.push(tmp_msg);
                to_read--;
            }
        }
		return tmp;
	}

	virtual bool IsNilext(specpaxos::vr::proto::RequestMessage msg) {
		string op = msg.req().op().substr(0, opLength);
		return IsAppend(op);
	}

	/*
	 * The AppUpcall encompasses both makedurable and read upcalls to the storage system
	 * If this is a nilext operation, then the operation is added to the durability log
	 * If this is a read operation, the readRes which is an out parameter contains the result of the read
	 * If the read requires a sync, syncOrder is set to true.
	 * Note that clients do not send the non-nilext operations to the durability server; they are
	 * immediately ordered by sending to consensus.
	*/

	virtual void AppUpcall(specpaxos::vr::proto::RequestMessage msg, bool &syncOrder, string &readRes) {
		syncOrder = false;

		size_t totalLen = msg.req().op().size();
		string op = msg.req().op().substr(0, opLength);
		string remaining = msg.req().op().substr(opLength, totalLen - opLength);

		std::pair<uint64_t, uint64_t> tableKey = std::make_pair(
				msg.req().clientid(), msg.req().clientreqid());

		if (IsAppend(op)) {
			durabilityLog.insert_or_assign(tableKey, std::make_pair(durLogIndex, msg));
			readRes = "durable-ack";
			durLogIndex++;
		} else if (IsRead(op)) {
			int blockToRead = std::stoi(remaining);
			// Notice("Read for block-%d", blockToRead);
			if (blockToRead < totalBlocks) {
				readRes = read(blockToRead);
			} else {
				syncOrder = true;
				readRes = "ordernowread!";
			}
		} else {
			Panic("Unknown operation to file-append store app %s", msg.req().op().c_str());
		}
	};

	virtual void clearDurabilityLog() {
		durabilityLog.clear();
		durLogIndex = 0;
	};

	virtual void clearQueue() {
		int to_read = queue.read_available();
		specpaxos::vr::proto::RequestMessage tmp_msg;
		while (to_read) {
			queue.pop(tmp_msg);
			to_read--;
		}
		if (queue.read_available()) {
			clearQueue();
		}
	}

	// Used during recovery and viewchange
	virtual void addToDurabilityLogInOrder(std::vector<Request> requests) {
		//Notice("addToDurabilityLogInOrder");
		for (auto it : requests) {
			std::pair<uint64_t, uint64_t> tableKey = std::make_pair(it.clientid(),
					it.clientreqid());
			specpaxos::vr::proto::RequestMessage *requestMessage = new  specpaxos::vr::proto::RequestMessage();
			requestMessage->set_allocated_req(&it);
			durabilityLog.insert_or_assign(tableKey, std::make_pair(durLogIndex++, *requestMessage));
			//Notice("%lu,%lu:", requestMessage->req().clientid(), requestMessage->req().clientreqid());
		}
	};

	// get the durability log in order.
	// Used during recovery and viewchange
	virtual std::vector<Request> GetDurabilityLogInOrder() {
		std::vector<std::pair<uint64_t, specpaxos::vr::proto::RequestMessage>> toSort;
		for (auto &it : durabilityLog) {
			toSort.push_back(std::make_pair(it.second.first, it.second.second));
		}

		// we sort the durability log by position.
		sort(toSort.begin(), toSort.end(),
				[=](
						std::pair<uint64_t, specpaxos::vr::proto::RequestMessage> &a,
						std::pair<uint64_t, specpaxos::vr::proto::RequestMessage> &b) {
					return a.first < b.first;
				}
		);

		std::vector<Request> toReturn;
		for (auto &it : toSort) {
			toReturn.push_back(it.second.req());
			Notice("DL: %lu, %lu, %lu", it.first, it.second.req().clientid(), it.second.req().clientreqid());
		}
		return toReturn;
	};

	// Invoke callback on all replicas
	// This is called when a request is committed (either synchronously or in background)
    virtual void ReplicaUpcall(opnum_t opnum, const Request &req, string &str2,
                               void *arg = nullptr, void *ret = nullptr) {

    	size_t totalLen = req.op().size();
    	string op = req.op().substr(0, opLength);
		string remaining = req.op().substr(opLength, totalLen - opLength);

    	if(!IsRead(op)) {
			apply(remaining);
			str2 = "";
			durabilityLog.erase(std::make_pair(req.clientid(), req.clientreqid()));
    	} else {
    		// populate read result
    		str2 = read(std::stoi(remaining));
    	}
    };

    // Rollback callback on failed speculative operations
    virtual void RollbackUpcall(opnum_t current, opnum_t to, const std::map<opnum_t, string> &opMap) { };

    virtual void CommitUpcall(opnum_t) { };

    // Invoke call back for unreplicated operations run on only one replica
    virtual void UnloggedUpcall(const string &str1, string &str2) { };
};

class Replica : public TransportReceiver
{
public:
    Replica(const Configuration &config, int groupIdx, int replicaIdx,
            bool initialize, Transport *transport, AppReplica *app);
    virtual ~Replica();

protected:
    void LeaderUpcall(specpaxos::vr::proto::RequestMessage msg, bool &syncOrder, string &readRes);
    void ReplicaUpcall(opnum_t opnum, const Request &req, string &res,
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
