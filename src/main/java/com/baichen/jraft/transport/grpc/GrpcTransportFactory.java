package com.baichen.jraft.transport.grpc;

import com.baichen.jraft.exception.JRaftException;
import com.baichen.jraft.transport.*;
import com.baichen.jraft.value.NodeInfo;

public class GrpcTransportFactory implements TransportFactory {

    @Override
    public TransportServer createServer(TransportOptions options, NodeInfo node) {

        GrpcTransportServer server = new GrpcTransportServer(node);
        Class exceptionHandlerClass = options.getExceptionHandlerClass();
        if (exceptionHandlerClass != null) {
            try {
                server.setExceptionHandler((ExceptionHandler) exceptionHandlerClass.newInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                throw new JRaftException(e);
            }
        }

        return server;
    }

    @Override
    public TransportClient createClient(TransportOptions options, NodeInfo node) {

        GrpcTransportClient client = new GrpcTransportClient(node);
        return client;
    }

    @Override
    public String getType() {
        return "grpc";
    }
}
