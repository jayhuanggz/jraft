package com.baichen.jraft.value;

public enum AppendRequestSessionState {

    CREATED, PENDING, COMMITTED, APPLIED, DESTROYED;
}
