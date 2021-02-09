package com.baichen.jraft.handler;

import com.baichen.jraft.Server;

public class NoopMessageHandlerPipelineConfigurer implements MessageHandlerPipelineConfigurer {
    @Override
    public void configure(MessageHandlerPipeline pipeline, Server server) {

    }
}
