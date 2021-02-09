package com.baichen.jraft;

import com.baichen.jraft.handler.MessageHandlerPipelineConfigurer;
import com.baichen.jraft.handler.NoopMessageHandlerPipelineConfigurer;
import com.baichen.jraft.impl.AtomicRequestIdGenerator;
import com.baichen.jraft.impl.HeartbeatTimerImpl;
import com.baichen.jraft.impl.RandomElectionTimer;
import com.baichen.jraft.impl.ServerImpl;
import com.baichen.jraft.log.factory.DefaultLogFactory;
import com.baichen.jraft.log.factory.LogFactory;
import com.baichen.jraft.log.factory.LogStorageFactory;
import com.baichen.jraft.membership.PeerFactory;
import com.baichen.jraft.membership.PeerFactoryImpl;
import com.baichen.jraft.meta.MetaStorageOptions;
import com.baichen.jraft.meta.StateStorageFactory;
import com.baichen.jraft.options.JRaftOptions;
import com.baichen.jraft.transport.TransportFactory;
import com.baichen.jraft.transport.TransportOptions;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ServiceLoader;
import java.util.function.Function;

public class JRaftServerBuilder {


    private static ServiceLoader<LogStorageFactory> LOG_FACTORY_LOADER
            = ServiceLoader.load(LogStorageFactory.class);

    private static ServiceLoader<StateStorageFactory> STATE_STORAGE_FACTORY_LOADER
            = ServiceLoader.load(StateStorageFactory.class);


    private static ServiceLoader<TransportFactory> TRANSPORT_FACTORY_LOADER
            = ServiceLoader.load(TransportFactory.class);


    private final JRaftOptions options;

    private Function<JRaftOptions, HeartbeatTimer> heartbeatTimerFactory;

    private Function<JRaftOptions, ElectionTimer> electionTimerFactory;

    private Function<JRaftOptions, LogFactory> logFactoryFactory;

    private Function<JRaftOptions, StateStorageFactory> stateStorageFactoryFactory;

    private MessageHandlerPipelineConfigurer messageHandlerPipelineConfigurer;

    private TransportFactory transportFactory;

    private PeerFactory peerFactory;

    private RequestIdGenerator requestIdGenerator;


    public JRaftServerBuilder(JRaftOptions options) {
        this.options = options;
        heartbeatTimerFactory = (o) -> new HeartbeatTimerImpl(o.getHeartbeatIntervalMs());
        electionTimerFactory = (o) -> new RandomElectionTimer(o.getElectionTimeoutMinMs(), o.getElectionTimeoutMaxMs());
        logFactoryFactory = (o) -> new DefaultLogFactory(loadLogStorageFactories());
        stateStorageFactoryFactory = (o) -> createStateStorageFactory(o.getMeta());
        transportFactory = loadTransportFactory(options.getTransport());
        peerFactory = new PeerFactoryImpl();
        requestIdGenerator = new AtomicRequestIdGenerator();
        messageHandlerPipelineConfigurer = new NoopMessageHandlerPipelineConfigurer();

    }

    private Collection<LogStorageFactory> loadLogStorageFactories() {
        Iterator<LogStorageFactory> iterator = LOG_FACTORY_LOADER.iterator();

        Collection<LogStorageFactory> result = new LinkedList<>();
        while (iterator.hasNext()) {
            result.add(iterator.next());
        }
        return result;

    }

    private StateStorageFactory createStateStorageFactory(MetaStorageOptions options) {
        Iterator<StateStorageFactory> iterator = STATE_STORAGE_FACTORY_LOADER.iterator();

        while (iterator.hasNext()) {

            StateStorageFactory factory = iterator.next();
            if (options.getType().equals(factory.getType())) {
                return factory;
            }

        }

        throw new IllegalArgumentException("No " + StateStorageFactory.class.getName() + " found!");

    }

    private TransportFactory loadTransportFactory(TransportOptions options) {
        Iterator<TransportFactory> iterator = TRANSPORT_FACTORY_LOADER.iterator();

        while (iterator.hasNext()) {

            TransportFactory factory = iterator.next();
            if (options.getType().equals(factory.getType())) {
                return factory;
            }

        }
        return null;
    }


    public JRaftServerBuilder heartbeatTimer(Function<JRaftOptions, HeartbeatTimer> heartbeatTimerFactory) {
        this.heartbeatTimerFactory = heartbeatTimerFactory;
        return this;
    }

    public JRaftServerBuilder electionTimer(Function<JRaftOptions, ElectionTimer> electionTimerFactory) {
        this.electionTimerFactory = electionTimerFactory;
        return this;
    }


    public JRaftServerBuilder stateStorageFactory(Function<JRaftOptions, StateStorageFactory> stateStorageFactoryFactory) {
        this.stateStorageFactoryFactory = stateStorageFactoryFactory;
        return this;
    }

    public JRaftServerBuilder logFactory(Function<JRaftOptions, LogFactory> logFactoryFactory) {
        this.logFactoryFactory = logFactoryFactory;
        return this;
    }

    public JRaftServerBuilder messageHandlerPipelineConfigurer(MessageHandlerPipelineConfigurer messageHandlerPipelineConfigurer) {
        this.messageHandlerPipelineConfigurer = messageHandlerPipelineConfigurer;
        return this;
    }

    public JRaftServerBuilder transportFactory(TransportFactory transportFactory) {
        this.transportFactory = transportFactory;
        return this;
    }

    public JRaftServerBuilder peerFactory(PeerFactory peerFactory) {
        this.peerFactory = peerFactory;
        return this;
    }

    public JRaftServerBuilder requestIdGenerator(RequestIdGenerator requestIdGenerator) {
        this.requestIdGenerator = requestIdGenerator;
        return this;
    }


    public Server build() {

        LogFactory logFactory = logFactoryFactory.apply(options);

        StateStorageFactory stateStorageFactory = stateStorageFactoryFactory.apply(options);


        ServerImpl server = new ServerImpl(options, logFactory,
                stateStorageFactory,
                heartbeatTimerFactory, electionTimerFactory,
                transportFactory, peerFactory, messageHandlerPipelineConfigurer);
        server.setRequestIdGenerator(requestIdGenerator);
        return server;
    }
}
