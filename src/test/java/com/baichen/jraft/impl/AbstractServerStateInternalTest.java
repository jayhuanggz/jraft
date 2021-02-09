package com.baichen.jraft.impl;

import com.baichen.jraft.Server;
import com.baichen.jraft.log.LogManager;
import com.baichen.jraft.log.LogStorage;
import com.baichen.jraft.log.SimpleLogManager;
import com.baichen.jraft.meta.StateStorage;
import com.baichen.jraft.test.InMemoryLogStorage;
import com.baichen.jraft.test.InMemoryStateStorage;
import com.baichen.jraft.value.NodeInfo;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public class AbstractServerStateInternalTest {

    @Mock
    protected Server server;

    protected LogStorage logStorage;

    protected StateStorage stateStorage;

    protected NodeInfo nodeInfo;

    protected LogManager logManager;


    @Before
    public void init() {
        MockitoAnnotations.openMocks(this.getClass());

        logStorage = new InMemoryLogStorage();

        logManager = new SimpleLogManager(logStorage);

        stateStorage = new InMemoryStateStorage();

        nodeInfo = new NodeInfo();
        nodeInfo.setId("node1");
        nodeInfo.setHost("127.0.0.1");
        nodeInfo.setPort(1000);
        nodeInfo.setName("Node 1");
        when(server.getNodeInfo()).thenReturn(nodeInfo);

    }
}
