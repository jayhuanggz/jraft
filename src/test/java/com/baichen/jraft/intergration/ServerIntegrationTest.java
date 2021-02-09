package com.baichen.jraft.intergration;

import com.baichen.jraft.JRaftServerBuilder;
import com.baichen.jraft.Server;
import com.baichen.jraft.impl.ServerImpl;
import com.baichen.jraft.log.model.LogEntry;
import com.baichen.jraft.model.AppendRequestSession;
import com.baichen.jraft.options.JRaftOptions;
import com.baichen.jraft.value.ServerState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

@RunWith(Parameterized.class)
public class ServerIntegrationTest {

    private ServerImpl server1;

    private ServerImpl server2;

    private ServerImpl server3;


    @Parameterized.Parameters
    public static Object[][] data() {
        return new Object[10][0];
    }


    @Before
    public void init() throws Exception {

        createDataFile("test_jraft_data_1");
        createDataFile("test_jraft_meta_1");
        createDataFile("test_jraft_data_2");
        createDataFile("test_jraft_meta_2");
        createDataFile("test_jraft_data_3");
        createDataFile("test_jraft_meta_3");


        server1 = (ServerImpl) startServer("test-server1.yml");
        server2 = (ServerImpl) startServer("test-server2.yml");
        server3 = (ServerImpl) startServer("test-server3.yml");

    }

    private Server startServer(String config) throws Exception {
        Yaml yaml = new Yaml(new Constructor(JRaftOptions.class));
        JRaftOptions options = yaml.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(config));
        JRaftServerBuilder builder = new JRaftServerBuilder(options);
        Server server = builder.build();
        return server;
    }


    @After
    public void tearDown() {
        try {
            if (server1 != null) {
                server1.destroy();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        try {
            if (server2 != null) {
                server2.destroy();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        try {
            if (server3 != null) {
                server3.destroy();
            }
        } catch (Throwable e) {
            e.printStackTrace();
        }

        deleteDataFile("test_jraft_data_1");
        deleteDataFile("test_jraft_meta_1");
        deleteDataFile("test_jraft_data_2");
        deleteDataFile("test_jraft_meta_2");
        deleteDataFile("test_jraft_data_3");
        deleteDataFile("test_jraft_meta_3");

    }

    private void createDataFile(String file) throws IOException {
        File dbFile = new File(file);
        if (dbFile.exists()) {
            Files.walk(dbFile.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    private void deleteDataFile(String file) {

        File dbFile = new File("file");
        if (dbFile.exists()) {
            try {
                Files.walk(dbFile.toPath())
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testLeaderElection() throws InterruptedException {
        server1.start();
        server2.start();
        server3.start();


        server1.transitionState(ServerState.CANDIDATE);

        System.out.println("Wait 3 seconds for leader election");

        Thread.sleep(3000);

        Assert.assertTrue(server1.isLeader());

    }


    @Test
    public void testLeaderElectionTimeout() throws InterruptedException {
        server1.start();
        server2.start();

        server1.transitionState(ServerState.CANDIDATE);
        int electionTimeout = server1.getOptions().getElectionTimeoutMaxMs();

        System.out.println("Wait " + (electionTimeout + 1000) + "ms for election timeout");
        Thread.sleep(electionTimeout + 1000);

        if (server1.getState() != ServerState.CANDIDATE) {
            Assert.assertEquals(ServerState.CANDIDATE, server2.getState());
        }

    }

    @Test
    public void testAppendAndReplication() throws InterruptedException, ExecutionException {
        server1.start();
        server2.start();
        server3.start();

        server1.transitionState(ServerState.CANDIDATE);
        System.out.println("Wait 3 seconds for leader election");

        Thread.sleep(3000);

        String command = "log1";

        long start = System.currentTimeMillis();
        AppendRequestSession request = server1.append(command.getBytes());
        request.await();
        long end = System.currentTimeMillis();
        System.out.println("Append spent: " + (end - start) + "ms......");
        Assert.assertEquals(request.getLog().getIndex(), server1.getLastLogIndex());

        LogEntry log1 = server1.getLastLog();
        Assert.assertNotNull(log1);
        Assert.assertEquals(command, new String(log1.getCommand()));

        LogEntry log2 = server2.getLogStorage().findByIndex(server2.getLastLogIndex());
        LogEntry log3 = server3.getLogStorage().findByIndex(server3.getLastLogIndex());

        if (log2 == null) {
            Assert.assertNotNull(log3);
            Assert.assertEquals(command, new String(log3.getCommand()));
        } else {
            Assert.assertNotNull(log2);
            Assert.assertEquals(command, new String(log2.getCommand()));
        }


    }


    @Test
    public void
    testAppendWhenSomeFollowerIsDown() throws InterruptedException, ExecutionException {
        server1.start();
        server2.start();
        server3.start();

        server1.transitionState(ServerState.CANDIDATE);
        System.out.println("Wait 3 seconds for leader election");

        Thread.sleep(3000);

        server3.destroy();
        System.out.println("Wait 1 second for server3 to destroy");

        Thread.sleep(1000);

        String command = "log1";

        AppendRequestSession request = server1.append(command.getBytes());
        request.await();

        // server1 and server2 received majority(2), commit index updated
        Assert.assertEquals(1, server1.getVolatileState().getCommitIndex());
        Assert.assertEquals(1, server2.getVolatileState().getCommitIndex());

        Assert.assertEquals(0, server3.getVolatileState().getCommitIndex());

        LogEntry log2 = server2.getLogStorage().findByIndex(server2.getLogStorage().getLastLogIndex());
        Assert.assertEquals(command, new String(log2.getCommand()));

    }

    @Test
    public void testAppendWhenMajorityFollowersAreDown() throws InterruptedException, ExecutionException {
        server1.start();
        server2.start();
        server3.start();

        server1.transitionState(ServerState.CANDIDATE);
        System.out.println("Wait 3 seconds for leader election");

        Thread.sleep(3000);

        server3.destroy();
        server2.destroy();
        System.out.println("Wait 1 second for servers to destroy");

        Thread.sleep(1000);

        String command = "log1";

        AppendRequestSession request = server1.append(command.getBytes());
        try {
            request.await(5, TimeUnit.SECONDS);
            Assert.fail();
        } catch (TimeoutException e) {
            Assert.assertEquals(0, server1.getVolatileState().getCommitIndex());
            Assert.assertEquals(0, server2.getVolatileState().getCommitIndex());
            Assert.assertEquals(0, server3.getVolatileState().getCommitIndex());
        }
    }


    @Test
    public void testAppendWhenMajorityFollowersAreDownAndUpLater() throws Exception {
        server1.start();
        server2.start();
        server3.start();

        server1.transitionState(ServerState.CANDIDATE);
        System.out.println("Wait 3 seconds for leader election");

        Thread.sleep(3000);

        server3.destroy();
        server2.destroy();
        System.out.println("Wait 1 second for servers to destroy");

        Thread.sleep(1000);

        String command = "log1";

        // append 1000 logs to server1 while server2 and server3 are down
        int count = 1000;
        for (int i = 0; i < count; i++) {
            server1.append(command.getBytes());
        }

        Thread.sleep(5000);
        Assert.assertEquals(0, server1.getVolatileState().getCommitIndex());
        Assert.assertEquals(0, server2.getVolatileState().getCommitIndex());
        Assert.assertEquals(0, server3.getVolatileState().getCommitIndex());
        // start server2 and server3
        server2 = (ServerImpl) startServer("test-server2.yml");

        server3 = (ServerImpl) startServer("test-server3.yml");

        server2.start();
        server3.start();
        Thread.sleep(5000);

        server1.append(command.getBytes());
        Thread.sleep(5000);
        Assert.assertEquals(count + 1, server1.getVolatileState().getCommitIndex());
        Assert.assertEquals(count + 1, server2.getVolatileState().getCommitIndex());
        Assert.assertEquals(count + 1, server3.getVolatileState().getCommitIndex());


        Assert.assertEquals(count + 1, server1.getLogStorage().getLastLogIndex());
        Assert.assertEquals(count + 1, server2.getLogStorage().getLastLogIndex());
        Assert.assertEquals(count + 1, server3.getLogStorage().getLastLogIndex());

        for (int i = 1; i <= count + 1; i++) {
            Assert.assertNotNull(server1.getLogStorage().findByIndex(i));
            Assert.assertNotNull(server2.getLogStorage().findByIndex(i));
            Assert.assertNotNull(server3.getLogStorage().findByIndex(i));

        }


    }


    @Test
    public void testManyAppends() throws Exception {
        server1.start();
        server2.start();
        server3.start();

        server1.transitionState(ServerState.CANDIDATE);
        System.out.println("Wait 3 seconds for leader election");

        Thread.sleep(3000);

        int count = 100000;
        long start = System.currentTimeMillis();

        int threads = 10;

        BlockingQueue<AppendRequestSession> sessions = new LinkedBlockingQueue<>();

        for (int i = 0; i < threads; i++) {
            new Thread(() -> {
                for (int j = 0; j < count / threads; j++) {
                    AppendRequestSession session = server1.append(new byte[0]);
                    sessions.offer(session);
                }

            }).start();
        }

        int committed = 0;
        while (committed < count) {
            AppendRequestSession session = sessions.poll();
            if (session != null) {
                session.await();
                committed++;
            }

        }

        long end = System.currentTimeMillis();
        System.out.println("Append spent: " + (end - start) + "ms......");


        Thread.sleep(5000);

        for (int i = 1; i <= count; i++) {
            LogEntry log = server1.getLogStorage().findByIndex(i);
            Assert.assertNotNull("server1 missing log index: " + i, log);

        }
        for (int i = 1; i <= count; i++) {
            LogEntry log = server2.getLogStorage().findByIndex(i);
            Assert.assertNotNull("server2 missing log index: " + i, log);

        }
        for (int i = 1; i <= count; i++) {
            LogEntry log = server3.getLogStorage().findByIndex(i);
            Assert.assertNotNull("server3 missing log index: " + i, log);

        }
    }
}
