package com.baichen.jraft.impl;

import com.baichen.jraft.*;
import com.baichen.jraft.exception.InvalidServerStateException;
import com.baichen.jraft.exception.JRaftException;
import com.baichen.jraft.handler.*;
import com.baichen.jraft.log.LogBatcher;
import com.baichen.jraft.log.LogCache;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.factory.LogFactory;
import com.baichen.jraft.log.impl.LogManagerImpl;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.log.options.LogOptions;
import com.baichen.jraft.membership.Peer;
import com.baichen.jraft.membership.PeerFactory;
import com.baichen.jraft.membership.PeerListener;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.meta.StateStorageFactory;
import com.baichen.jraft.model.*;
import com.baichen.jraft.options.JRaftOptions;
import com.baichen.jraft.transport.RpcResult;
import com.baichen.jraft.transport.TransportFactory;
import com.baichen.jraft.transport.TransportServer;
import com.baichen.jraft.transport.TransportServerService;
import com.baichen.jraft.util.LockUtils;
import com.baichen.jraft.util.NamedThreadFactory;
import com.baichen.jraft.value.NodeInfo;
import com.baichen.jraft.value.ServerState;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.ThreadHints;
import org.apache.logging.log4j.Logger;
import sun.misc.Perf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class ServerImpl implements Server, TransportServerService, PeerListener {

    private static final Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(ServerImpl.class);

    private static final int READY_STATE_NOT_STARTED = 0;

    private static final int READY_STATE_STARTING = 1;

    private static final int READY_STATE_READY = 2;

    private static final int READY_STATE_DESTROYED = 3;

    private static final int EVENT_APPEND = 1;

    private static final int EVENT_ON_APPEND_RESULT = 2;

    private static final int EVENT_ON_VOTE_RESULT = 3;

    private static final int EVENT_TRANSITION_STATE = 4;

    private static final int EVENT_HEARTBEAT = 5;


    private final JRaftOptions options;

    private LogFactory logFactory;

    private ServerStateInternal stateInternal;

    private LogCache logCache;

    private LogStorage logStorage;

    private LogBatcher logBatcher;

    private StateStorageFactory stateStorageFactory;

    private StateStorage stateStorage;

    private VolatileState volatileState;

    private ServerState state;

    private RequestIdGenerator requestIdGenerator;

    private Function<JRaftOptions, HeartbeatTimer> heartbeatTimerFactory;

    private HeartbeatTimer heartbeatTimer;

    private Function<JRaftOptions, ElectionTimer> electionTimerFactory;

    private int majorityCount;

    private AtomicInteger runState;

    private List<Peer> peers;

    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Lock readLock = lock.readLock();

    private Lock writeLock = lock.writeLock();

    private List<ServerListener> subs = new CopyOnWriteArrayList<>();

    private TransportFactory transportFactory;

    private TransportServer transportServer;

    private PeerFactory peerFactory;

    private MessageHandlerPipeline messageHandlerPipeline;

    private volatile String leaderId;

    private Disruptor<Event> disruptor;

    private RingBuffer<Event> ringBuffer;

    private static final EventTranslatorTwoArg<Event, Integer, Object> TRANSLATOR =
            new EventTranslatorTwoArg<Event, Integer, Object>() {
                @Override
                public void translateTo(Event event, long sequence, Integer type, Object data) {
                    event.release();
                    event.type = type;
                    event.data = data;
                }
            };

    public ServerImpl(JRaftOptions options,
                      LogFactory logFactory,
                      StateStorageFactory stateStorageFactory,
                      Function<JRaftOptions, HeartbeatTimer> heartbeatTimerFactory,
                      Function<JRaftOptions, ElectionTimer> electionTimerFactory,
                      TransportFactory transportFactory,
                      PeerFactory peerFactory,
                      MessageHandlerPipelineConfigurer messageHandlerPipelineConfigurer

    ) {
        this.options = options;
        this.logFactory = logFactory;
        this.stateStorageFactory = stateStorageFactory;
        this.electionTimerFactory = electionTimerFactory;
        this.heartbeatTimerFactory = heartbeatTimerFactory;
        this.runState = new AtomicInteger(READY_STATE_NOT_STARTED);
        this.transportFactory = transportFactory;
        this.peerFactory = peerFactory;
        messageHandlerPipeline = new MessageHandlerPipelineImpl();
        messageHandlerPipeline.addHandler(new StaleTermHandler());
        messageHandlerPipeline.addHandler(new CompareTermsForCandidateHandler());
        messageHandlerPipeline.addHandler(new RejectOlderTermHandler());
        messageHandlerPipelineConfigurer.configure(messageHandlerPipeline, this);


    }

    public JRaftOptions getOptions() {
        return options;
    }

    public void setRequestIdGenerator(RequestIdGenerator requestIdGenerator) {
        this.requestIdGenerator = requestIdGenerator;
    }


    public void setState(ServerState state) {
        this.state = state;
    }

    @Override
    public LogStorage getLogStorage() {
        return logStorage;
    }

    @Override
    public StateStorage getStateStorage() {
        return stateStorage;
    }

    public VolatileState getVolatileState() {
        return volatileState;
    }

    @Override
    public List<Peer> getPeers() {
        return peers;
    }

    @Override
    public NodeInfo getNodeInfo() {
        return options.getNodeInfo();
    }

    @Override
    public ServerState getState() {
        return state;
    }

    @Override
    public void transitionState(ServerState state) {

        checkIsReady();
        if (this.state == state) {
            return;
        }

        publishEvent(EVENT_TRANSITION_STATE, state);
    }

    @Override
    public int getMajorityCount() {
        return majorityCount;
    }

    @Override
    public AppendRequestSession append(byte[] command) {

        checkIsReady();

        if (state != ServerState.LEADER) {
            throw new InvalidServerStateException("Invalid server state: " + state);
        }
        AppendRequestSession session = ((Leader) stateInternal).createAppendSession(command);
        publishEvent(EVENT_APPEND, session);
        return session;
    }

    private void publishEvent(int type, Object data) {


        while (!ringBuffer.tryPublishEvent(TRANSLATOR, type, data)) {
            ThreadHints.onSpinWait();
        }
    }

    @Override
    public void subscribe(ServerListener sub) {

        writeLock.lock();

        try {
            subs.add(sub);
        } finally {
            writeLock.unlock();
        }

    }

    @Override
    public void unsubscribe(ServerListener sub) {
        writeLock.lock();

        try {
            subs.remove(sub);
        } finally {
            writeLock.unlock();
        }

    }

    @Override
    public boolean isLeader() {
        return leaderId != null && leaderId.equals(getNodeInfo().getId());
    }

    @Override
    public String getLeaderId() {
        return leaderId;
    }

    @Override
    public LogEntry getLastLog() {
        return stateInternal.getLastLog();
    }

    @Override
    public int getLastLogIndex() {
        return stateInternal.getLastLogIndex();
    }

    @Override
    public int getLastLogTerm() {
        return stateInternal.getLastLogTerm();
    }

    @Override
    public void start() {

        writeLock.lock();

        try {
            if (runState.compareAndSet(READY_STATE_NOT_STARTED, READY_STATE_STARTING)) {

                LogOptions logOptions = options.getLog();

                logBatcher = logFactory.createBatcher(logOptions.getBatch());
                logBatcher.start();

                logCache = logFactory.createCache(logOptions.getCache());
                logCache.start();

                logStorage = logFactory.createStorage(logOptions.getStorage());
                logStorage.start();


                stateStorage = stateStorageFactory.create(options.getMeta());
                stateStorage.start();

                messageHandlerPipeline.start();

                List<NodeInfo> peerNodes = options.getPeers();
                peers = Collections.emptyList();
                if (peerNodes != null && !peerNodes.isEmpty()) {
                    peers = new ArrayList<>(peerNodes.size());
                    for (NodeInfo peerNode : peerNodes) {
                        Peer peer = peerFactory.createPeer(peerNode, transportFactory);
                        peer.subscribe(this);
                        peer.start();
                        peers.add(peer);

                    }
                }
                int totalNodeCount = peers.size() + 1;
                majorityCount = totalNodeCount / 2;
                int mod = totalNodeCount % 2;
                if (mod > 0) {
                    majorityCount++;
                }
                volatileState = new VolatileState(0, 0);


                disruptor = new Disruptor<>(new EventFactory<Event>() {
                    @Override
                    public Event newInstance() {
                        return new Event();
                    }
                },
                        1024 * 1024,
                        new NamedThreadFactory("JRaft-ServerImpl", true),
                        ProducerType.MULTI, new BusySpinWaitStrategy());


                disruptor.handleEventsWith(new ServerEventHandler(this));
                ringBuffer = disruptor.start();


                transportServer = transportFactory.createServer(options.getTransport(), getNodeInfo());
                transportServer.registerService(this);
                transportServer.start();

                for (ServerListener sub : subs) {
                    sub.onStarted(this);
                }


                becomeFollower();

                runState.set(READY_STATE_READY);
            }


        } finally {
            writeLock.unlock();
        }

    }

    private void handleEvent(Event event) {

        try {
            checkIsReady();

            final int type = event.type;
            switch (type) {
                case EVENT_APPEND:
                    if (state != ServerState.LEADER) {
                        throw new InvalidServerStateException("Invalid server state: " + state);
                    }
                    ((Leader) stateInternal).append((AppendRequestSession) event.data);
                    break;
                case EVENT_ON_APPEND_RESULT:
                    RpcResult appendResult = (RpcResult) event.data;

                    handleMessage(appendResult);

                    if (state == ServerState.LEADER) {
                        ((Leader) stateInternal).onAppendReplyReceived(appendResult);
                    }
                    break;
                case EVENT_HEARTBEAT:
                    if (state == ServerState.LEADER && stateInternal != null) {
                        ((LeaderImpl) stateInternal).sendHeartBeat();
                    }
                    break;
                case EVENT_TRANSITION_STATE:

                    ServerState newState = (ServerState) event.data;

                    if (newState != state) {
                        switch (newState) {
                            case FOLLOWER:
                                becomeFollower();
                                break;
                            case LEADER:
                                becomeLeader();
                                break;
                            case CANDIDATE:
                                becomeCandidate();
                                break;
                        }
                    }
                    break;
                case EVENT_ON_VOTE_RESULT:

                    RpcResult voteResult = (RpcResult) event.data;

                    handleMessage(voteResult);

                    if (state == ServerState.CANDIDATE) {
                        ((Candidate) stateInternal).handleVoteReplyFromPeer(voteResult.getNodeId());

                    }
                    break;
            }
        } catch (Throwable e) {
            LOGGER.warn("Error handle server event......", e);
        }

    }


    private void checkIsReady() {
        if (runState.get() != READY_STATE_READY) {
            throw new JRaftException("Server is not ready!");
        }
    }


    private void becomeLeader() {
        cleanForStateTransition();

        LOGGER.info("Server {} is becoming Leader......", this.getNodeInfo().getId());
        state = ServerState.LEADER;
        this.leaderId = getNodeInfo().getId();
        stateInternal = new LeaderImpl(this,
                () -> logFactory.createManager(logStorage, logCache, logBatcher, options.getLog()),
                (peer) -> logFactory.createLogStream(peer),
                stateStorage, volatileState, requestIdGenerator);
        stateInternal.start();
        startHeartbeatTimer();
    }


    private void becomeCandidate() {
        cleanForStateTransition();
        LOGGER.info("Server {} is becoming Candidate......", this.getNodeInfo().getId());

        state = ServerState.CANDIDATE;

        stateInternal = new CandidateImpl(this, logStorage, stateStorage,
                volatileState, requestIdGenerator, electionTimerFactory.apply(options));
        stateInternal.start();
    }

    private void becomeFollower() {
        cleanForStateTransition();

        LOGGER.info("Server {} is becoming Follower......", this.getNodeInfo().getId());

        state = ServerState.FOLLOWER;


        stateInternal = new FollowerImpl(this, logStorage, stateStorage, volatileState, electionTimerFactory.apply(options));
        stateInternal.start();
    }

    private void startHeartbeatTimer() {
        heartbeatTimer = heartbeatTimerFactory.apply(options);
        heartbeatTimer.subscribe(this::sendHeartbeat);
        heartbeatTimer.start();
    }


    private void sendHeartbeat() {
        checkIsReady();

        if (state == ServerState.LEADER && stateInternal != null) {
            publishEvent(EVENT_HEARTBEAT, null);
        }

    }

    private void cleanForStateTransition() {
        if (stateInternal != null) {
            LOGGER.info("{} destroying state {}......", getNodeInfo().getId(), state);

            try {
                stateInternal.destroy();
            } catch (Throwable e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                stateInternal = null;
            }

        }


        if (heartbeatTimer != null) {
            heartbeatTimer.destroy();
            heartbeatTimer = null;
        }


    }


    @Override
    public void destroy() {

        writeLock.lock();

        try {

            if (runState.get() == READY_STATE_DESTROYED) {
                throw new IllegalStateException("Server has been destroyed!");
            }

            runState.set(READY_STATE_DESTROYED);

            disruptor.shutdown();
            try {
                cleanForStateTransition();
            } catch (Throwable e) {
                LOGGER.warn(e.getMessage(), e);
            }

            if (stateStorage != null) {
                stateStorage.destroy();
            }

            if (messageHandlerPipeline != null) {
                messageHandlerPipeline.destroy();
            }

            if (logStorage != null) {
                try {
                    logStorage.destroy();
                } catch (Throwable e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }

            if (logCache != null) {
                try {
                    logCache.destroy();
                } catch (Throwable e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }

            if (logBatcher != null) {
                try {
                    logBatcher.destroy();
                } catch (Throwable e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
            if (transportServer != null) {
                transportServer.destroy();
            }
            if (peers != null) {
                for (Peer peer : peers) {
                    peer.destroy();
                }
            }


            try {
                for (ServerListener sub : subs) {
                    sub.onDestroyed(this);
                }

            } finally {
                heartbeatTimerFactory = null;
                electionTimerFactory = null;
                stateStorageFactory = null;
                logFactory = null;
                subs.clear();
            }

        } finally {
            writeLock.unlock();
        }

    }

    private RpcResult handleMessage(RpcMessage message) {
        MessageHandlerContextImpl context = new MessageHandlerContextImpl(this, message);
        try {
            messageHandlerPipeline.handle(context);
            return context.getResult();
        } finally {
            context.release();
        }

    }

    @Override
    public RpcResult handleAppendRequest(AppendRequest request) {


        LockUtils.lock("ServerImpl.handleAppendRequest", writeLock, 5, TimeUnit.SECONDS, 0, 3);
        try {

            if (runState.get() == READY_STATE_DESTROYED) {
                return null;
            }
            RpcResult result = handleMessage(request);

            if (result != null) {
                return result;
            }

            if (state == ServerState.FOLLOWER) {
                if (!request.getLeaderId().equals(leaderId)) {
                    leaderId = request.getLeaderId();
                }
                return ((Follower) stateInternal).handleAppendReceived(request);
            } else {
                result = new RpcResult();
                result.setCode(RpcResult.Codes.REJECT);
                result.setMsg("rejected");
                result.setNodeId(getNodeInfo().getId());
                result.setRequestId(request.getRequestId());
                result.setTerm(stateStorage.getState().getTerm());
                return result;
            }
        } finally {
            writeLock.unlock();
        }

    }

    @Override
    public RpcResult handleVoteRequest(VoteRequest request) {

        if (runState.get() == READY_STATE_DESTROYED) {
            return null;
        }
        LockUtils.lock("ServerImpl.handleVoteRequest", writeLock, 5, TimeUnit.SECONDS, 0, 3);
        try {

            if (runState.get() == READY_STATE_DESTROYED) {
                return null;
            }
            RpcResult result = handleMessage(request);

            if (result != null) {
                return result;
            }

            if (state == ServerState.FOLLOWER) {
                return ((Follower) stateInternal).handleRequestVoteReceived(request);
            } else {
                result = new RpcResult();
                result.setCode(RpcResult.Codes.VOTE_REJECT);
                result.setMsg("rejected");
                result.setNodeId(getNodeInfo().getId());
                result.setRequestId(request.getRequestId());
                result.setTerm(stateStorage.getState().getTerm());
                return result;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void onVoteReply(RpcResult result) {


        checkIsReady();

        publishEvent(EVENT_ON_VOTE_RESULT, result);

    }

    @Override
    public void onAppendReply(RpcResult result) {

        if (runState.get() == READY_STATE_DESTROYED) {
            return;
        }
        publishEvent(EVENT_ON_APPEND_RESULT, result);


    }

    private static final class ServerEventHandler implements EventHandler<Event> {

        private ServerImpl server;

        public ServerEventHandler(ServerImpl server) {
            this.server = server;
        }

        @Override
        public void onEvent(Event event, long sequence, boolean endOfBatch) throws Exception {
            server.handleEvent(event);
        }
    }

    private static final class Event {

        private int type;

        private Object data;

        void release() {
            data = null;
        }

    }
}
