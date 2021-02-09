package com.baichen.jraft.handler;

import com.baichen.jraft.Lifecycle;

public interface MessageHandler extends Lifecycle {

    void handle(MessageHandlerContext context);

}
