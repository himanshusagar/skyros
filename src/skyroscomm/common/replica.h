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
	bool isLeader;
    int opLength = 1;
    int keyLength = 24;

    ConcurrentHashMap<string, std::string> kvStore;
    std::unordered_map<string, CXID> lastUpdateToKey;

    //the value in durability log is a tuple of position, request. key is clientid, clientrequestid
    ConcurrentHashMap<CXID, std::pair<uint64_t, specpaxos::vr::proto::RequestMessage>> durabilityLog;
    std::unordered_map<uint64_t, uint64_t> durLogClientTable;
    boost::lockfree::spsc_queue<CXID> committedEntries{10000};

    int durLogIndex = 0;

    void apply(string key, string value) {
    	// Notice("Applying %s to store", key.c_str());
    	kvStore.insert_or_assign(key, value);
    }

    string getFromStore(string key) {
    	if(kvStore.find(key) != kvStore.end())
			return (kvStore.find(key))->second;

		return "NOTFOUND";
    }

    bool IsGet(string op) {
    	return !op.compare("r") || !op.compare("R");
    }

	bool IsNonNilext(string op) {
    	return !op.compare("e") || !op.compare("E");
    }

    bool IsSet(string op) {
    	return !op.compare("i") || !op.compare("I")
    			|| !op.compare("u") || !op.compare("U")
				|| !op.compare("e") || !op.compare("E");
    }

public:
	boost::lockfree::spsc_queue<specpaxos::vr::proto::RequestMessage> queue{10000000};
	ConcurrentHashMap<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
    
    AppReplica(): kvStore(10*1000*1000), durabilityLog(10*1000*1000) {
    	lastUpdateToKey.reserve(10*1000*1000);
    };

    virtual ~AppReplica() { };
	virtual void AddToQueue(specpaxos::vr::proto::RequestMessage msg) {
		while(!queue.push(msg));
	}

	virtual std::queue<specpaxos::vr::proto::RequestMessage> GetAndDeleteFromQueue() {
		std::queue<specpaxos::vr::proto::RequestMessage> tmp;
		specpaxos::vr::proto::RequestMessage tmp_msg;
        // limiting the number of items dequeued to what is available to read now.
        int to_read = queue.read_available();
        while (to_read) {
            if (queue.pop(tmp_msg)) {
                tmp.push(tmp_msg);
                to_read--;
            }
        }
		return tmp;
	}

	virtual bool ExposesState(specpaxos::vr::proto::RequestMessage msg) {
		string op = msg.req().op().substr(0, opLength);
		return IsGet(op);
	}

	// Invoke callback on the leader, with the option to replicate on success
	virtual void AppUpcall(specpaxos::vr::proto::RequestMessage msg, bool leader, bool &syncOrder, string &readRes) {
		syncOrder = false;
		isLeader = leader;
		static int batch_window = 0;
		batch_window++;
		string op = msg.req().op().substr(0, opLength);
		string kvKey = msg.req().op().substr(opLength, keyLength);
		int otherLength = opLength + keyLength;
		int valLength = msg.req().op().length() - otherLength;
		string kvVal = msg.req().op().substr(otherLength, valLength);

		std::pair<uint64_t, uint64_t> tableKey = std::make_pair(
				msg.req().clientid(), msg.req().clientreqid());

		if (batch_window % 10 == 0) {
			batch_window = 0;
			int to_commit = committedEntries.read_available();
			while (to_commit) {
				CXID clientRequestID;
				if (committedEntries.pop(clientRequestID)) {
					//erase from durability log
					durabilityLog.erase(clientRequestID);
					to_commit--;
					if (durLogClientTable.find(clientRequestID.first) != durLogClientTable.end()) {
						//client id is there in the table
						// so check if we have seen a more recent request.
						if (durLogClientTable[clientRequestID.first] < clientRequestID.second) {
							durLogClientTable.insert_or_assign(clientRequestID.first,
									clientRequestID.second);
						}
					} else {
						//client id is not there in the table; so insert this.
						durLogClientTable.insert_or_assign(clientRequestID.first,
								clientRequestID.second);
					}
				}
			}
		}

		bool alreadySeen = false;
		if (durLogClientTable.find(tableKey.first) != durLogClientTable.end()) {
			//client id is there in the table
			// so check if we have seen a more recent request.
			if (durLogClientTable[tableKey.first] >= tableKey.second) {
				alreadySeen = true;
			}
		}

		if (IsSet(op) && !IsNonNilext(op)) {
			//Notice("Adding to durabiity set and lastUpdateToKey %lu,%lu: %s,%s", msg.req().clientid(),  msg.req().clientreqid(), op.c_str(), kvKey.c_str());
			if (!alreadySeen) {
				durabilityLog.insert_or_assign(tableKey, std::make_pair(durLogIndex++,msg));
				lastUpdateToKey.insert_or_assign(kvKey, tableKey);
				durLogClientTable.insert_or_assign(tableKey.first, tableKey.second);
				readRes = "durable-ack";
			} else {
				readRes = "dup-ack";
			}

		} else if (IsSet(op) && IsNonNilext(op)) {
			if (!alreadySeen) {
				if(lastUpdateToKey.find(kvKey) != lastUpdateToKey.end()) {
					std::pair<uint64_t, uint64_t> index = lastUpdateToKey[kvKey];
					syncOrder = (durabilityLog.find(index) != durabilityLog.end());
				}

				if(!syncOrder) {
					durabilityLog.insert_or_assign(tableKey, std::make_pair(durLogIndex++,msg));
					lastUpdateToKey.insert_or_assign(kvKey, tableKey);
					durLogClientTable.insert_or_assign(tableKey.first, tableKey.second);
					if (isLeader) {
						apply(kvKey, kvVal);
					}
					readRes = "durable-ack2";
				} else {
					readRes = "ordernowconflictingwrite!";
					if (leader) {
						lastUpdateToKey.insert_or_assign(kvKey, tableKey);
						durabilityLog.insert_or_assign(tableKey, std::make_pair(2,msg));
						durLogClientTable.insert_or_assign(tableKey.first, tableKey.second);
					}
				}
			} else{
				readRes = "dup-ack";
			}
		} else if (IsGet(op)) {
			assert(0); // for curp-kv workloads, we are going to do write-only workloads
		} else {
			Panic("Unknown operation to KV store app %s", msg.req().op().c_str());
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
			Notice("Clearing queue again");
			clearQueue();
		}
		//Notice("Clearing queue");
	}

	virtual void addToDurabilityLogInOrder(std::vector<Request> requests) {
		//Notice("addToDurabilityLogInOrder");
		for (auto it : requests) {
			std::pair<uint64_t, uint64_t> tableKey = std::make_pair(it.clientid(),
					it.clientreqid());
			string kvKey = it.op().substr(opLength, keyLength);
			specpaxos::vr::proto::RequestMessage *requestMessage = new  specpaxos::vr::proto::RequestMessage();
			requestMessage->set_allocated_req(&it);
			durabilityLog.insert_or_assign(tableKey, std::make_pair(durLogIndex++, *requestMessage));
			lastUpdateToKey.insert_or_assign(kvKey, tableKey);
			//Notice("%lu,%lu:", requestMessage->req().clientid(), requestMessage->req().clientreqid());
		}
	};

	// get the durability log in order.
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
    virtual void ReplicaUpcall(opnum_t opnum, const Request &req, string &str2,
                               void *arg = nullptr, void *ret = nullptr) {
    	string op = req.op().substr(0, opLength);
    	string kvKey = req.op().substr(opLength, keyLength);
		CXID tableKey = std::make_pair(req.clientid(), req.clientreqid());
    	if(!IsGet(op)) {
			int otherLength = opLength + keyLength;
			int valLength = req.op().length() - otherLength;
			string kvVal = req.op().substr(otherLength, valLength);
			//Notice("Applying and deleting from durability set %lu,%lu: %s,%s", req.clientid(),  req.clientreqid(), op.c_str(), kvKey.c_str());
			if(!IsNonNilext(op))
				apply(kvKey, kvVal);
			else {
				if(!isLeader)
					apply(kvKey, kvVal);
				else {
					if(req.syncread() == 1) {
						// leader had a conflicting request and did not apply in the fast path
						// apply now
						apply(kvKey, kvVal);
					}
				}
			}

			str2 = "";
			//durabilityLog.erase(tableKey);
			while(!committedEntries.push(tableKey));
			// outstandingOps.erase(tableKey);
    	} else {
    		// populate read result
    		str2 = getFromStore(kvKey);
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
    void LeaderUpcall(specpaxos::vr::proto::RequestMessage msg, bool leader, bool &syncOrder, string &readRes);
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
