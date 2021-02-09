package com.baichen.jraft.transport;

import java.io.Serializable;
import java.util.Properties;

public class TransportOptions implements Serializable {

    private String type = "grpc";

    private Class exceptionHandlerClass;


    private Properties serverOptions = new Properties();

    private Properties clientOptions = new Properties();

    public Properties getClientOptions() {
        return clientOptions;
    }

    public void setClientOptions(Properties clientOptions) {
        this.clientOptions = clientOptions;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Class getExceptionHandlerClass() {
        return exceptionHandlerClass;
    }

    public void setExceptionHandlerClass(Class exceptionHandlerClass) {
        this.exceptionHandlerClass = exceptionHandlerClass;
    }


    public Properties getServerOptions() {
        return serverOptions;
    }

    public void setServerOptions(Properties serverOptions) {
        this.serverOptions = serverOptions;
    }
}
