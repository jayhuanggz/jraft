package com.baichen.jraft.log.value;

import java.io.Serializable;
import java.util.Objects;

public final class LogEntryId implements Serializable {

    private int term;

    private int index;

    public LogEntryId(int term, int index) {
        this.term = term;
        this.index = index;
    }

    public int getTerm() {
        return term;
    }

    public int getIndex() {
        return index;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntryId that = (LogEntryId) o;
        return term == that.term &&
                index == that.index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, index);
    }
}
