package com.baichen.jraft.transport.grpc;

import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.transport.RpcResult;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class GrpcUtils {

    public static RpcResult toJavaResult(com.baichen.jraft.transport.grpc.RpcResult rpc) {
        RpcResult result = new RpcResult();
        result.setCode(rpc.getCode());
        result.setMsg(rpc.getMsg());
        result.setCommitIndex(rpc.getCommitIndex());
        result.setRequestId(rpc.getRequestId());
        result.setTerm(rpc.getTerm());
        result.setNodeId(rpc.getNodeId());
        return result;
    }


    public static com.baichen.jraft.model.AppendRequest toJavaAppendRequest(com.baichen.jraft.transport.grpc.AppendRequest rpc) {
        com.baichen.jraft.model.AppendRequest result = new com.baichen.jraft.model.AppendRequest();
        result.setRequestId(rpc.getRequestId());
        result.setLeaderCommit(rpc.getLeaderCommit());
        result.setPrevLogIndex(rpc.getPrevLogIndex());
        result.setPrevLogTerm(rpc.getPrevLogTerm());
        result.setLeaderId(rpc.getLeaderId());
        result.setTerm(rpc.getTerm());
        List<Log> rpcLogs = rpc.getLogsList();
        if (rpcLogs != null && !rpcLogs.isEmpty()) {
            List<LogEntry> logs = new ArrayList<>(rpcLogs.size());
            for (Log rpcLog : rpcLogs) {
                logs.add(new LogEntry(rpcLog.getTerm(), rpcLog.getIndex(), rpcLog.getCommand().toByteArray()));
            }
            result.setLogs(logs);
        }

        return result;
    }


    public static com.baichen.jraft.model.VoteRequest toJavaVoteRequest(com.baichen.jraft.transport.grpc.VoteRequest rpc) {
        return new com.baichen.jraft.model.VoteRequest(rpc.getRequestId(),
                rpc.getTerm(),
                rpc.getCandidateId(),
                rpc.getLastLogIndex(),
                rpc.getLastLogTerm()
        );
    }

    public static com.baichen.jraft.transport.grpc.AppendRequest toRpcAppendRequest(com.baichen.jraft.model.AppendRequest request) {
        com.baichen.jraft.transport.grpc.AppendRequest.Builder builder = com.baichen.jraft.transport.grpc.AppendRequest.newBuilder();
        builder.setLeaderCommit(request.getLeaderCommit());
        builder.setTerm(request.getTerm());
        builder.setPrevLogTerm(request.getPrevLogTerm());
        builder.setPrevLogIndex(request.getPrevLogIndex());
        builder.setLeaderId(request.getLeaderId());
        builder.setRequestId(request.getRequestId());
        if (request.getLogs() != null && !request.getLogs().isEmpty()) {
            List<LogEntry> logs = request.getLogs();
            for (LogEntry log : logs) {
                Log.Builder logBuilder = Log.newBuilder();
                builder.addLogs(logBuilder.setIndex(log.getIndex())
                        .setTerm(log.getTerm())
                        .setCommand(ByteString.copyFrom(log.getCommand()))
                        .build());
            }
        }

        return builder.build();
    }


    public static com.baichen.jraft.transport.grpc.VoteRequest toRpcVoteRequest(com.baichen.jraft.model.VoteRequest request) {
        com.baichen.jraft.transport.grpc.VoteRequest.Builder builder = com.baichen.jraft.transport.grpc.VoteRequest.newBuilder();
        builder.setTerm(request.getTerm());
        builder.setLastLogIndex(request.getLastLogIndex());
        builder.setLastLogTerm(request.getLastLogTerm());
        builder.setCandidateId(request.getCandidateId());
        builder.setRequestId(request.getRequestId());


        return builder.build();
    }

    public static com.baichen.jraft.transport.grpc.RpcResult toRpcResult(com.baichen.jraft.transport.RpcResult result) {
        com.baichen.jraft.transport.grpc.RpcResult.Builder builder = com.baichen.jraft.transport.grpc.RpcResult.newBuilder();

        builder.setTerm(result.getTerm());
        builder.setCode(result.getCode());
        builder.setRequestId(result.getRequestId());
        builder.setNodeId(result.getNodeId());
        if (result.getMsg() != null) {
            builder.setMsg(result.getMsg());
        }
        builder.setCommitIndex(result.getCommitIndex());
        return builder.build();

    }
}
