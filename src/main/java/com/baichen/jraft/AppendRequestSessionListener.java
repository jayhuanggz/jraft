package com.baichen.jraft;

import com.baichen.jraft.model.AppendRequestSession;

public interface AppendRequestSessionListener {

    void onTimeout(AppendRequestSession session);

    void onCommitted(AppendRequestSession session);

    void onApplied(AppendRequestSession session);

}
