package com.zju.minisql.common.replica;

public class ReplicaResult {

    private final boolean success;
    private final String message;

    private ReplicaResult(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public static ReplicaResult ok(String message) {
        return new ReplicaResult(true, message);
    }

    public static ReplicaResult fail(String message) {
        return new ReplicaResult(false, message);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getMessage() {
        return message;
    }
}
