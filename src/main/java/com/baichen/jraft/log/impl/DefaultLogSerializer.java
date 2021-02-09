package com.baichen.jraft.log.impl;

import com.baichen.jraft.log.LogSerializer;
import com.baichen.jraft.log.model.LogEntry;

public class DefaultLogSerializer implements LogSerializer {

    @Override
    public LogEntry deserialize(int index, byte[] data) {
        int term = getInt(data);
        return doDeserialize(data, index, term);
    }


    @Override
    public LogEntry deserialize(byte[] indexData, byte[] data) {

        int index = getInt(indexData);

        int term = getInt(data);

        return doDeserialize(data, index, term);
    }

    private LogEntry doDeserialize(byte[] data, int index, int term) {
        byte[] command = null;
        if (data.length > 4) {
            command = new byte[data.length - 4];
            System.arraycopy(data, 4, command, 0, data.length - 4);
        }

        return new LogEntry(term, index, command);
    }


    private int getInt(byte[] data) {
        return data[3] & 0xFF |
                (data[2] & 0xFF) << 8 |
                (data[1] & 0xFF) << 16 |
                (data[0] & 0xFF) << 24;
    }

    @Override
    public byte[] serialize(LogEntry log) {

        byte[] command = log.getCommand();
        int commandLength = command == null ? 0 : command.length;
        int total = 4 + commandLength;

        byte[] result = new byte[total];

        for (int i = 0; i < total; i++) {
            if (i <= 3) {
                result[i] = (byte) (log.getTerm() >>> (8 * (3 - i)));
            } else {
                result[i] = command[i - 4];
            }
        }

        return result;
    }
}
