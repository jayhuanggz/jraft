package com.baichen.jraft.options;

import com.baichen.jraft.log.options.LogOptions;
import com.baichen.jraft.meta.MetaStorageOptions;
import com.baichen.jraft.transport.TransportOptions;
import com.baichen.jraft.value.NodeInfo;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class JRaftOptions implements Serializable {

    private NodeInfo nodeInfo;

    private int electionTimeoutMinMs;

    private int electionTimeoutMaxMs;

    private int heartbeatIntervalMs;

    private List<NodeInfo> peers = Collections.emptyList();

    private MetaStorageOptions meta = new MetaStorageOptions();

    private LogOptions log = new LogOptions();

    private TransportOptions transport = new TransportOptions();

    public TransportOptions getTransport() {
        return transport;
    }

    public void setTransport(TransportOptions transport) {
        this.transport = transport;
    }

    public LogOptions getLog() {
        return log;
    }

    public void setLog(LogOptions log) {
        this.log = log;
    }

    public List<NodeInfo> getPeers() {
        return peers;
    }

    public void setPeers(List<NodeInfo> peers) {
        this.peers = peers;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }

    public void setNodeInfo(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public int getElectionTimeoutMinMs() {
        return electionTimeoutMinMs;
    }

    public void setElectionTimeoutMinMs(int electionTimeoutMinMs) {
        this.electionTimeoutMinMs = electionTimeoutMinMs;
    }

    public int getElectionTimeoutMaxMs() {
        return electionTimeoutMaxMs;
    }

    public void setElectionTimeoutMaxMs(int electionTimeoutMaxMs) {
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public MetaStorageOptions getMeta() {
        return meta;
    }

    public void setMeta(MetaStorageOptions meta) {
        this.meta = meta;
    }
}
