package com.baichen.jraft.model;

import java.io.Serializable;

public class RpcMessage implements Serializable {

    protected int term;

    protected int requestId;

    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }
}
