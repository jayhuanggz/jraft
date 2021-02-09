package com.baichen.jraft.impl;

import com.baichen.jraft.RequestIdGenerator;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicRequestIdGenerator implements RequestIdGenerator {

    private final AtomicInteger value = new AtomicInteger(1);

    @Override
    public int next() {
        return value.getAndIncrement();
    }
}
