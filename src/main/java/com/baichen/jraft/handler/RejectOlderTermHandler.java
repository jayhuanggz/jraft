package com.baichen.jraft.handler;

import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.model.VoteRequest;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.transport.RpcResult;

public class RejectOlderTermHandler implements MessageHandler {
    @Override
    public void handle(MessageHandlerContext context) {
        if (context.getMessage() instanceof AppendRequest || context.getMessage() instanceof VoteRequest) {
            StateStorage stateStorage = context.getServer().getStateStorage();
            int term = stateStorage.getState().getTerm();

            boolean isOlderTerm = term > context.getMessage().getTerm();
            if (isOlderTerm) {
                RpcResult result = new RpcResult();
                result.setNodeId(context.getServer().getNodeInfo().getId());
                result.setTerm(term);
                result.setRequestId(context.getMessage().getRequestId());
                result.setMsg("rejected");
                result.setCode(RpcResult.Codes.REJECT);
                context.setResult(result);
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
