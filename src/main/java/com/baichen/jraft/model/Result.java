package com.baichen.jraft.model;

import com.baichen.jraft.value.ServerState;

public class Result<T extends Reply> {

    private T reply;

    private boolean staleTerm;

    private ServerState newState;

    public Result(T reply, boolean staleTerm) {
        this.reply = reply;
        this.staleTerm = staleTerm;
    }

    public Result(T reply) {
        this.reply = reply;
    }

    public ServerState getNewState() {
        return newState;
    }

    public void setNewState(ServerState newState) {
        this.newState = newState;
    }

    public Reply getReply() {
        return reply;
    }

    public boolean isStaleTerm() {
        return staleTerm;
    }
}
