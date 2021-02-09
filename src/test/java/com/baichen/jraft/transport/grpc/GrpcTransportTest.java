package com.baichen.jraft.transport.grpc;


import com.baichen.jraft.model.VoteRequest;
import com.baichen.jraft.transport.RpcResult;
import com.baichen.jraft.transport.TransportServerService;
import com.baichen.jraft.transport.exception.TransportClientRequestTimeoutException;
import com.baichen.jraft.value.NodeInfo;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(MockitoJUnitRunner.class)
public class GrpcTransportTest {

    private GrpcTransportClient client;

    private GrpcTransportServer server;

    private NodeInfo node;

    @Mock
    private TransportServerService transportServerService;

    private ExecutorService futureExecutor;

    @Before
    public void init() {
        node = new NodeInfo();
        node.setId("node1");
        node.setHost("127.0.0.1");
        node.setPort(1122);

        client = new GrpcTransportClient(node);
        server = new GrpcTransportServer(node);
        server.registerService(transportServerService);

        futureExecutor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() {
        futureExecutor.shutdownNow();

        client.destroy();
        server.destroy();
    }


    @Test
    public void testClientSendRequestWhenServerIsDown() throws InterruptedException {


        client.start();
        ListenableFuture<RpcResult> future = client.sendVoteRequest(new VoteRequest(1, 1, "nodeId", 0, 0));

        try {
            future.get();
            Assert.fail();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            Assert.assertEquals(StatusRuntimeException.class, cause.getClass());
        }


    }

    @Test
    public void testClientSendRequestsAllFail() throws InterruptedException {

        client.start();

        int count = 10000;

        CountDownLatch latch = new CountDownLatch(count);

        for (int i = 1; i <= count; i++) {
            ListenableFuture<RpcResult> future = client.sendVoteRequest(createVoteRequest(i));
            try {
                future.get();
                Assert.fail();
            } catch (ExecutionException e) {
                latch.countDown();
            }
        }

        latch.await();
    }

    @Test
    public void testClientSendRequestsWhenServerStartsInTheMiddle() throws InterruptedException {

        client.start();

        int count = 5000;

        CountDownLatch latch = new CountDownLatch(count);

        for (int i = 1; i <= count; i++) {
            ListenableFuture<RpcResult> future = client.sendVoteRequest(createVoteRequest(i));
            try {
                future.get();
                Assert.fail();
            } catch (ExecutionException e) {
                latch.countDown();
            }
        }

        latch.await();
        AtomicInteger requestId = new AtomicInteger(0);
        Mockito.when(transportServerService.handleVoteRequest(Mockito.any(VoteRequest.class))).then(mock -> {
            VoteRequest request = mock.getArgument(0);
            Assert.assertEquals(requestId.incrementAndGet(), request.getRequestId());
            RpcResult result = new RpcResult();
            result.setNodeId(node.getId());
            result.setRequestId(request.getRequestId());
            return result;
        });
        final CountDownLatch secondLatch = new CountDownLatch(count);

        final Thread mainThread = Thread.currentThread();
        server.startWithFuture().addListener(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {

            }

            for (int i = 1; i <= count; i++) {
                ListenableFuture<RpcResult> future = client.sendVoteRequest(createVoteRequest(i));
                try {
                    future.get();
                    secondLatch.countDown();
                } catch (Exception e) {
                    try {
                        Assert.fail();
                    } finally {
                        mainThread.interrupt();

                    }
                }
            }

        }, futureExecutor);
        try {
            secondLatch.await();
        } catch (InterruptedException e) {
        }
        Mockito.verify(transportServerService, Mockito.times(count)).handleVoteRequest(Mockito.any(VoteRequest.class));


    }


    @Test
    public void testClientSendRequestsWhenServerRestartsInTheMiddle() throws InterruptedException, ExecutionException {

        client.start();
        server.startWithFuture().get();
        int count = 100;

        for (int i = 1; i <= count; i++) {
            client.sendVoteRequest(createVoteRequest(i));

        }

        server.destroy();

        count = 100;

        for (int i = 1; i <= count; i++) {
            ListenableFuture<RpcResult> future = client.sendVoteRequest(createVoteRequest(i));
            try {
                future.get();
            } catch (ExecutionException e) {

            }
        }
        Thread.sleep(1000);
        server = new GrpcTransportServer(node);
        server.registerService(transportServerService);

        AtomicInteger requestId = new AtomicInteger(0);
        Mockito.when(transportServerService.handleVoteRequest(Mockito.any(VoteRequest.class))).then(mock -> {
            VoteRequest request = mock.getArgument(0);
            Assert.assertEquals(requestId.incrementAndGet(), request.getRequestId());
            RpcResult result = new RpcResult();
            result.setNodeId(node.getId());
            result.setRequestId(request.getRequestId());
            return result;
        });
        final CountDownLatch secondLatch = new CountDownLatch(count);

        final Thread mainThread = Thread.currentThread();
        server.startWithFuture().get();

        Thread.sleep(5000);

        for (int i = 1; i <= count; i++) {
            ListenableFuture<RpcResult> future = client.sendVoteRequest(createVoteRequest(i));
            try {
                future.get();
                secondLatch.countDown();
            } catch (Exception e) {
                try {
                    Assert.fail();
                } finally {
                    mainThread.interrupt();
                }
            }
        }


        try {
            secondLatch.await();
        } catch (InterruptedException e) {
        }
        Mockito.verify(transportServerService, Mockito.times(count)).handleVoteRequest(Mockito.any(VoteRequest.class));


    }

    @Test
    public void testClientRequestTimeout() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        client.start();
        server.startWithFuture().addListener(() -> {

            ListenableFuture<RpcResult> future = client.sendVoteRequest(createVoteRequest(1));
            try {
                future.get();
                Assert.fail();
            } catch (InterruptedException e) {
                Assert.fail();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (!(cause instanceof TransportClientRequestTimeoutException)) {
                    Assert.fail("Exception should be TransportClientRequestTimeoutException");
                }
            } finally {
                latch.countDown();
            }

        }, futureExecutor);

        Mockito.when(transportServerService.handleVoteRequest(Mockito.any(VoteRequest.class))).then(mock -> {

            Thread.sleep(5000);
            VoteRequest request = mock.getArgument(0);
            RpcResult result = new RpcResult();
            result.setNodeId(node.getId());
            result.setRequestId(request.getRequestId());
            return result;
        });
        latch.await();
    }

    @Test
    public void testClientSendRequestsInMultipleThreads() throws InterruptedException {

        client.start();

        Mockito.when(transportServerService.handleVoteRequest(Mockito.any(VoteRequest.class))).then(mock -> {
            VoteRequest request = mock.getArgument(0);
            RpcResult result = new RpcResult();
            result.setNodeId(node.getId());
            result.setRequestId(request.getRequestId());
            return result;
        });
        int count = 10000;

        final CountDownLatch latch = new CountDownLatch(count);
        class Sender extends Thread {

            private BlockingQueue<VoteRequest> requests = new LinkedBlockingQueue<>();

            private volatile boolean stopped = false;

            @Override
            public void run() {

                while (!stopped) {
                    VoteRequest request = this.requests.poll();
                    if (request != null) {
                        ListenableFuture<RpcResult> future = client.sendVoteRequest(request);
                        try {
                            RpcResult result = future.get();
                            Assert.assertEquals(request.getRequestId(), result.getRequestId());
                        } catch (Exception e) {
                            e.printStackTrace();
                            Assert.fail();
                        } finally {
                            latch.countDown();
                        }
                    }
                }
            }
        }
        final int threadCount = 100;

        final List<Sender> senders = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Sender sender = new Sender();
            senders.add(sender);
            sender.start();
        }
        server.startWithFuture().addListener(() -> {

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

            for (int i = 0; i < count; i++) {
                VoteRequest request = createVoteRequest(i + 1);
                Sender sender = senders.get(i % threadCount);
                sender.requests.offer(request);
            }
        }, futureExecutor);

        latch.await();
        for (Sender sender : senders) {
            sender.stopped = true;
            sender.join();
        }
    }

    @Test
    public void testSendManyMessages() throws InterruptedException, ExecutionException {

        client.start();
        server.start();

        final int count = 100000;
        CountDownLatch latch = new CountDownLatch(count);
        AtomicInteger currentRequestId = new AtomicInteger(0);
        Mockito.when(transportServerService.handleVoteRequest(Mockito.any(VoteRequest.class))).then(mock -> {
            VoteRequest request = mock.getArgument(0);
            Assert.assertEquals(currentRequestId.incrementAndGet(), request.getRequestId());
            RpcResult result = new RpcResult();
            result.setNodeId(node.getId());
            result.setRequestId(request.getRequestId());
            return result;

        });

        Thread.sleep(5000);
        AtomicInteger currentReplyRequestId = new AtomicInteger(0);

        class Sub implements Runnable {
            private final int index;

            Sub(int index) {
                this.index = index;
            }

            @Override
            public void run() {
                try {
                    Assert.assertEquals(index, currentReplyRequestId.incrementAndGet());
                } finally {
                    latch.countDown();
                }
            }
        }

        for (int i = 1; i <= count; i++) {
            client.sendVoteRequest(createVoteRequest(i)).addListener(new Sub(i), futureExecutor);


        }
        latch.await();


    }

    private VoteRequest createVoteRequest(int requestId) {
        return new VoteRequest(requestId, 1, "nodeId", 0, 0);
    }


}
