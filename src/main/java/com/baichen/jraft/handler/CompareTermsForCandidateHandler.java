package com.baichen.jraft.handler;

import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.RpcMessage;
import com.baichen.jraft.Server;
import com.baichen.jraft.value.ServerState;

public class CompareTermsForCandidateHandler implements MessageHandler {

    @Override
    public void handle(MessageHandlerContext context) {

        Server server = context.getServer();

        ServerState state = server.getState();

        RpcMessage message = context.getMessage();
        if (state == ServerState.CANDIDATE && message instanceof AppendRequest) {
            int currentTerm = server.getStateStorage().getState().getTerm();
            if (currentTerm <= context.getMessage().getTerm()) {
                server.transitionState(ServerState.FOLLOWER);
            }
        }


    }

    @Override
    public void start() {

    }

    @Override
    public void destroy() {

    }
}
