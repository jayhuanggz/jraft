package com.baichen.jraft.log.model;

import java.io.Serializable;
import java.util.Objects;

public class LogEntry implements Serializable {

    private int term;

    private int index;

    private byte[] command;

    public LogEntry(int term, int index, byte[] command) {
        this.term = term;
        this.index = index;
        this.command = command;
    }

    public LogEntry(int term, int index) {
        this.term = term;
        this.index = index;
    }

    LogEntry() {
    }

    public int getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public byte[] getCommand() {
        return command;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term &&
                index == logEntry.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, index);
    }
}
