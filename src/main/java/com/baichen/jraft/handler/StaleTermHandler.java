package com.baichen.jraft.handler;

import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.value.ServerState;

public class StaleTermHandler implements MessageHandler {

    @Override
    public void handle(MessageHandlerContext context) {

        StateStorage stateStorage = context.getServer().getStateStorage();
        int term = stateStorage.getState().getTerm();

        boolean stale = term < context.getMessage().getTerm();
        if (stale) {
            term = context.getMessage().getTerm();
            stateStorage.updateTerm(term);
            context.getServer().transitionState(ServerState.FOLLOWER);

        }
    }

    @Override
    public void start() {

    }

    @Override
    public void destroy() {
    }
}
