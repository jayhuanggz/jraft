package com.baichen.jraft.log.value;

public enum LogStreamState {

    NORMAL,

    FOLLOWER_INCONSISTENT,

    BLOCKED_BY_ERROR
}
