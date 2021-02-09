package com.baichen.jraft.handler;

import com.baichen.jraft.Lifecycle;

public interface MessageHandlerPipeline extends Lifecycle {

    void addHandler(MessageHandler handler);

    void handle(MessageHandlerContext context);
}
