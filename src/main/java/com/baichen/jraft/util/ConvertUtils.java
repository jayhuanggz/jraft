package com.baichen.jraft.util;

import java.nio.ByteBuffer;

public class ConvertUtils {

    public static byte[] intToBytes(int val) {
        return ByteBuffer.allocate(4).putInt(val).array();
    }

    public static int bytesToInt(byte[] data) {
        return data[3] & 0xFF |
                (data[2] & 0xFF) << 8 |
                (data[1] & 0xFF) << 16 |
                (data[0] & 0xFF) << 24;
    }
}
