package com.zju.minisql.common.replica;

import java.io.Serializable;

/**
 * 副本同步返回结果。
 */
public class ReplicaSyncAck implements Serializable {

    private boolean success;
    private boolean gapDetected;
    private long expectedLogIndex;
    private String message;

    public ReplicaSyncAck() {
    }

    public static ReplicaSyncAck ok(long expectedLogIndex, String message) {
        ReplicaSyncAck ack = new ReplicaSyncAck();
        ack.success = true;
        ack.gapDetected = false;
        ack.expectedLogIndex = expectedLogIndex;
        ack.message = message;
        return ack;
    }

    public static ReplicaSyncAck gap(long expectedLogIndex, String message) {
        ReplicaSyncAck ack = new ReplicaSyncAck();
        ack.success = false;
        ack.gapDetected = true;
        ack.expectedLogIndex = expectedLogIndex;
        ack.message = message;
        return ack;
    }

    public static ReplicaSyncAck fail(String message) {
        ReplicaSyncAck ack = new ReplicaSyncAck();
        ack.success = false;
        ack.gapDetected = false;
        ack.expectedLogIndex = -1L;
        ack.message = message;
        return ack;
    }

    public boolean isSuccess() {
        return success;
    }

    public boolean isGapDetected() {
        return gapDetected;
    }

    public long getExpectedLogIndex() {
        return expectedLogIndex;
    }

    public String getMessage() {
        return message;
    }
}
