package com.baichen.jraft.handler;

import com.baichen.jraft.Server;

public interface MessageHandlerPipelineConfigurer {

    void configure(MessageHandlerPipeline pipeline, Server server);
}
