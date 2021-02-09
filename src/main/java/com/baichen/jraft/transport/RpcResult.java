package com.baichen.jraft.transport;

import com.baichen.jraft.model.RpcMessage;

public class RpcResult extends RpcMessage {

    private String nodeId;

    private int code = Codes.OK;


    private int commitIndex;

    private String msg = "ok";

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }


    public static final class Codes {

        public static final int OK = 0;

        public static final int LOG_CONFLICT = 1001;

        public static final int VOTE_REJECT = 1002;

        public static final int REJECT = 1003;

        public static final int VOTE_REJECT_STALE_LOG = 1003;

        public static final int UNKNOWN = 500;

    }
}
