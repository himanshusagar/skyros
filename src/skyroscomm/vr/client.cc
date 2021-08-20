// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * vr/client.cc:
 *   Viewstamped Replication clinet
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

#include "common/client.h"
#include "common/request.pb.h"
#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "vr/client.h"
#include "vr/vr-proto.pb.h"

namespace specpaxos {
namespace vr {

VRClient::VRClient(const Configuration &config,
                   Transport *transport,
                   uint64_t clientid)
    : Client(config, transport, clientid)
{
    pendingRequest = NULL;
    pendingUnloggedRequest = NULL;
    lastReqId = 0;
    quorum = 0;
    oneRTTs = twoRTTs= threeRTTs = 0;
    requestTimeout = new Timeout(transport, 7000, [this]() {
            ResendRequest(0);
        });
    unloggedRequestTimeout = new Timeout(transport, 1000, [this]() {
            UnloggedRequestTimeoutCallback();
        });
}

VRClient::~VRClient()
{
    if (pendingRequest) {
        delete pendingRequest;
    }
    if (pendingUnloggedRequest) {
        delete pendingUnloggedRequest;
    }
    delete requestTimeout;
    delete unloggedRequestTimeout;
}

void
VRClient::Invoke(const string &request,
                 continuation_t continuation)
{
    // XXX Can only handle one pending request for now
    if (pendingRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    responses[lastReqId] = 0;
    neg_responses[lastReqId] = 0;
    leader_acked[lastReqId] = 0;
    uint64_t reqId = lastReqId;
    pendingRequest = new PendingRequest(request, reqId, continuation);
    quorum = config.FastQuorumSize();
    // Notice("Sending request %s", request.c_str());
    if(request.c_str()[0] == 'r' || request.c_str()[0] == 'R') {
        // only one response expected for reads
		// this is because in compat mode, even non-nilext can be processed in fast path 
    	quorum = 1;
    }

    SendRequest(0);
}

void
VRClient::InvokeUnlogged(int replicaIdx,
                         const string &request,
                         continuation_t continuation,
                         timeout_continuation_t timeoutContinuation,
                         uint32_t timeout)
{
    // XXX Can only handle one pending request for now
    if (pendingUnloggedRequest != NULL) {
        Panic("Client only supports one pending request");
    }

    ++lastReqId;
    uint64_t reqId = lastReqId;

    pendingUnloggedRequest = new PendingRequest(request, reqId, continuation);
    pendingUnloggedRequest->timeoutContinuation = timeoutContinuation;

    proto::UnloggedRequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingUnloggedRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingUnloggedRequest->clientReqId);

    ASSERT(!unloggedRequestTimeout->Active());
    unloggedRequestTimeout->SetTimeout(timeout);
    unloggedRequestTimeout->Start();

    transport->SendMessageToReplica(this, replicaIdx, reqMsg);
}

void
VRClient::SendRequest(int retry)
{
    proto::RequestMessage reqMsg;
    reqMsg.mutable_req()->set_op(pendingRequest->request);
    reqMsg.mutable_req()->set_clientid(clientid);
    reqMsg.mutable_req()->set_clientreqid(pendingRequest->clientReqId);
    reqMsg.mutable_req()->set_retry(retry);
    // XXX Try sending only to (what we think is) the leader first
    transport->SendMessageToAll(this, reqMsg);

    requestTimeout->Reset();
}

void
VRClient::ResendRequest(int retry)
{
    //Warning("Client timeout; resending request");
    SendRequest(retry);
}


void
VRClient::ReceiveMessage(const TransportAddress &remote,
                         const string &type,
                         const string &data,
                         void *meta_data)
{
    static proto::ReplyMessage reply;
    static proto::UnloggedReplyMessage unloggedReply;

    if (type == reply.GetTypeName()) {
        reply.ParseFromString(data);
        HandleReply(remote, reply);
    } else if (type == unloggedReply.GetTypeName()) {
        unloggedReply.ParseFromString(data);
        HandleUnloggedReply(remote, unloggedReply);
    } else {
        Client::ReceiveMessage(remote, type, data, NULL);
    }
}

void
VRClient::HandleReply(const TransportAddress &remote,
                      const proto::ReplyMessage &msg)
{

    if(msg.rejected()) {
    	neg_responses[msg.clientreqid()]++;
    } else {
    	responses[msg.clientreqid()]++;
    }
    // hack to ensure that view changes dont occur during performance experiments
    assert(msg.view() == 0); 

    if((uint64_t) config.GetLeaderIndex(msg.view()) == msg.replicaidx()) {
        leader_acked[msg.clientreqid()] = 1;
        if(msg.has_conflicting() && msg.conflicting()) {
        	quorum = 1;
        } else {
        	leader_fast_acked[msg.clientreqid()] = 1;
        }
    }

    if (pendingRequest == NULL) {
        // Warning("Received reply when no request was pending");
        return;
    }
    if (msg.clientreqid() != pendingRequest->clientReqId) {
        // Warning("Received reply for a different request");
        return;
    }

    Debug("Client received reply");

    if(responses[msg.clientreqid()] >= quorum
        && leader_acked[msg.clientreqid()]) {
        requestTimeout->Stop();
        PendingRequest *req = pendingRequest;
        pendingRequest = NULL;
        if (quorum == 1) {
        	if((req->request.c_str()[0] == 'e' || req->request.c_str()[0] == 'E')) {
        		assert(!leader_fast_acked[msg.clientreqid()]);
        	}
        	if(!leader_fast_acked[msg.clientreqid()]) {
        		twoRTTs++;
        	}
        	else{
        		oneRTTs++;
        	}
        }
        else {
        	assert(leader_fast_acked[msg.clientreqid()]);
        	oneRTTs++;
        }
        req->continuation(req->request, msg.reply());
        // Notice("Got response: %s", msg.reply().c_str());
        delete req;
	} else if (leader_fast_acked[msg.clientreqid()] && neg_responses[msg.clientreqid()] > (config.n - config.FastQuorumSize())) {
		//leader fast acked the request but followers did not; so the client has to resolve using a 3rtt protocol
		leader_acked[lastReqId] = 0;
		leader_fast_acked[lastReqId] = 0;
		responses[lastReqId] = 0;
		threeRTTs++;
		quorum = 1;
		//by setting this to negative n we ensure that it will not trigger multiple message resends.
		neg_responses[msg.clientreqid()] = -config.n;
		ResendRequest(1);
	}
}

void
VRClient::HandleUnloggedReply(const TransportAddress &remote,
                              const proto::UnloggedReplyMessage &msg)
{
    if (pendingUnloggedRequest == NULL) {
        Warning("Received unloggedReply when no request was pending");
        return;
    }

    Debug("Client received unloggedReply");

    unloggedRequestTimeout->Stop();

    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    req->continuation(req->request, msg.reply());
    delete req;
}

void
VRClient::UnloggedRequestTimeoutCallback()
{
    PendingRequest *req = pendingUnloggedRequest;
    pendingUnloggedRequest = NULL;

    Warning("Unlogged request timed out");

    unloggedRequestTimeout->Stop();

    req->timeoutContinuation(req->request);
}

} // namespace vr
} // namespace specpaxos
