package com.baichen.jraft;

import com.baichen.jraft.log.LogManager;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.membership.Peer;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.model.AppendRequestSession;
import com.baichen.jraft.value.NodeInfo;
import com.baichen.jraft.value.ServerState;

import java.util.List;

public interface Server extends Lifecycle {

    List<Peer> getPeers();

    NodeInfo getNodeInfo();

    ServerState getState();

    void transitionState(ServerState state);

    int getMajorityCount();

    LogStorage getLogStorage();

    StateStorage getStateStorage();

    AppendRequestSession append(byte[] command);

    void subscribe(ServerListener sub);

    void unsubscribe(ServerListener sub);

    boolean isLeader();

    String getLeaderId();

    LogEntry getLastLog();

    int getLastLogIndex();

    int getLastLogTerm();

}
