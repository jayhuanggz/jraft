package com.baichen.jraft.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class MessageHandlerPipelineImpl implements MessageHandlerPipeline {

    private static final Logger LOGGER = LogManager.getLogger(MessageHandlerPipelineImpl.class);

    private List<MessageHandler> handlers;

    private volatile MessageHandlerContext context;

    public MessageHandlerPipelineImpl() {
        this.handlers = new ArrayList<>();
    }

    @Override
    public void addHandler(MessageHandler handler) {
        this.handlers.add(handler);
    }

    @Override
    public void handle(MessageHandlerContext context) {
        for (MessageHandler handler : handlers) {
            handler.handle(context);
            if (context.isStopped()) {
                break;
            }
        }
    }


    @Override
    public void start() {
        for (MessageHandler handler : handlers) {
            try {
                handler.start();
            } catch (Throwable e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }

    }

    @Override
    public void destroy() {

        for (MessageHandler handler : handlers) {
            try {
                handler.destroy();
            } catch (Throwable e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        context = null;
    }
}
