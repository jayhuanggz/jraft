package com.baichen.jraft.log.impl;

import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.impl.LeaderImplTest;
import com.baichen.jraft.membership.Peer;
import com.baichen.jraft.log.value.LogStreamState;
import com.baichen.jraft.transport.RpcResult;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogStreamImplTest {

    private LogStreamImpl stream;

    @Mock
    private Peer peer;

    @Before
    public void init() {
        MockitoAnnotations.openMocks(LeaderImplTest.class);
        stream = new LogStreamImpl(peer);
        stream.start();

        SettableFuture<RpcResult> future = SettableFuture.create();
        future.set(new RpcResult());
        when(peer.appendEntries(any(AppendRequest.class))).thenReturn(future);
    }

    @After
    public void destroy() {
        stream.destroy();
    }

    @Test
    public void testAppend_successful() throws InterruptedException {

        AppendRequest request = AppendRequest.heartbeat(1, 1,
                "leader", 1, 1, 0);

        Assert.assertTrue(stream.append(request));

        CountDownLatch latch = new CountDownLatch(1);

        stream.getInflights().subscribe(inflight -> {
            try {
                Assert.assertEquals(1, inflight.getRequestId());
                Assert.assertTrue(inflight.isSuccess());
            } finally {
                latch.countDown();
            }
        });

        stream.onLogAccepted(1, 1, 1, 1);
        latch.await();
    }

    @Test
    public void testAppend_failedDueToInconsistency() throws InterruptedException {

        AppendRequest request = AppendRequest.heartbeat(1, 1,
                "leader", 1, 1, 1);

        Assert.assertTrue(stream.append(request));

        CountDownLatch latch = new CountDownLatch(1);

        stream.getInflights().subscribe(inflight -> {
            try {
                Assert.assertEquals(1, inflight.getRequestId());
                Assert.assertFalse(inflight.isSuccess());

            } finally {
                latch.countDown();
            }
        });

        stream.failOnInconsistency(1, 1, 1, 1);
        Assert.assertEquals(LogStreamState.FOLLOWER_INCONSISTENT, stream.getState());

        latch.await();
    }

    @Test
    public void testAppend_ifFollowerInconsistent() throws InterruptedException {

        AppendRequest request = AppendRequest.heartbeat(1, 1,
                "leader", 2, 1, 2);

        Assert.assertTrue(stream.append(request));


        stream.failOnInconsistency(1, 1, 1, 1);
        Assert.assertEquals(LogStreamState.FOLLOWER_INCONSISTENT, stream.getState());

        //success for first retry
        Assert.assertTrue(stream.append(AppendRequest.heartbeat(2, 1,
                "leader", 1, 1, 2)));

        //reject if there is an inflight
        Assert.assertFalse(stream.append(AppendRequest.heartbeat(3, 1,
                "leader", 2, 1, 2)));

    }

    @Test
    public void testOnLogAccepted_changeStateToNormalAndAcceptNewLogs() throws InterruptedException {

        AppendRequest request = AppendRequest.heartbeat(1, 1,
                "leader", 2, 1, 2);

        stream.append(request);


        stream.failOnInconsistency(1, 1, 1, 1);

        Assert.assertEquals(LogStreamState.FOLLOWER_INCONSISTENT, stream.getState());

        //send a retry
        Assert.assertTrue(stream.append(AppendRequest.heartbeat(2, 1,
                "leader", 1, 1, 2)));

        //the retry succeeded
        stream.onLogAccepted(2, 1, 1, 2);

        Assert.assertEquals(LogStreamState.NORMAL, stream.getState());

        Assert.assertTrue(stream.append(AppendRequest.heartbeat(3, 1,
                "leader", 3, 1, 2)));
    }

    @Test
    public void testAppend_manyLogsAndFailMiddleOne() throws InterruptedException {


        //append 1000 logs, index starting from 1
        for (int i = 1; i < 1001; i++) {
            int requestId = i;
            int logIndex = i;
            AppendRequest request = AppendRequest.heartbeat(requestId, 1,
                    "leader", logIndex, 1, 1);
            Assert.assertTrue(stream.append(request));
        }

        Set<Integer> failedIds = new HashSet<>(668);
        for (int i = 500; i < 1001; i++) {
            failedIds.add(i);
        }

        CountDownLatch latch = new CountDownLatch(501);
        stream.getInflights().subscribe(inflight -> {
            try {
                Assert.assertFalse(inflight.isSuccess());
                Assert.assertTrue("Invalid inflight: " + inflight.getRequestId(), failedIds.remove(inflight.getRequestId()));
            } finally {
                latch.countDown();
            }
        });


        // 1 to 499 logs may be have been lost as follower crash and recover immediately
        // then received log 500, which will be rejected due to inconsistency
        stream.failOnInconsistency(500, 500, 500, 1);
        latch.await();
        Assert.assertTrue(failedIds.isEmpty());

    }


}
