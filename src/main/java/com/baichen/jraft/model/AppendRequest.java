package com.baichen.jraft.model;

import com.baichen.jraft.log.model.LogEntry;

import java.util.Collections;
import java.util.List;

public class AppendRequest extends RpcMessage {


    private String leaderId;

    private int prevLogIndex;

    private int prevLogTerm;

    private List<LogEntry> logs;

    private int leaderCommit;


    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public List<LogEntry> getLogs() {
        return logs;
    }

    public void setLogs(List<LogEntry> logs) {
        this.logs = logs;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }


    public static AppendRequest append(int requestId, int term, String leaderId,
                                       int prevLogIndex, int prevLogTerm,
                                       int leaderCommit, List<LogEntry> logs) {
        AppendRequest request = new AppendRequest();
        request.setRequestId(requestId);
        request.setTerm(term);
        request.setLeaderId(leaderId);
        request.setLogs(logs);
        request.setPrevLogIndex(prevLogIndex);
        request.setPrevLogTerm(prevLogTerm);
        request.setLeaderCommit(leaderCommit);
        return request;
    }

    public static AppendRequest append(int requestId, int term, String leaderId,
                                       int prevLogIndex, int prevLogTerm,
                                       int leaderCommit, LogEntry log) {
        AppendRequest request = new AppendRequest();
        request.setRequestId(requestId);
        request.setTerm(term);
        request.setLeaderId(leaderId);
        request.setLogs(Collections.singletonList(log));
        request.setPrevLogIndex(prevLogIndex);
        request.setPrevLogTerm(prevLogTerm);
        request.setLeaderCommit(leaderCommit);
        return request;
    }

    public static AppendRequest heartbeat(int requestId, int term, String leaderId,
                                          int prevLogIndex, int prevLogTerm,
                                          int leaderCommit) {
        AppendRequest request = new AppendRequest();
        request.setRequestId(requestId);
        request.setTerm(term);
        request.setLeaderId(leaderId);
        request.setPrevLogIndex(prevLogIndex);
        request.setPrevLogTerm(prevLogTerm);
        request.setLeaderCommit(leaderCommit);
        return request;
    }


}
