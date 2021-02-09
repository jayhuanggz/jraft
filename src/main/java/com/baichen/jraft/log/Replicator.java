package com.baichen.jraft.log;

import com.baichen.jraft.model.AppendReply;
import com.baichen.jraft.model.AppendRequest;
import com.baichen.jraft.Lifecycle;

/**
 * Responsible for replicating logs sent from leader.
 */
public interface Replicator extends Lifecycle {

    AppendReply receiveAppend(AppendRequest request);


}
