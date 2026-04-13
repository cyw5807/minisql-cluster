package com.zju.minisql.common.rpc;

public class MockServiceImpl implements MockService {
    @Override
    public String ping(String message) {
        return "Pong: " + message;
    }

    @Override
    public int calculate(int a, int b) {
        return a + b;
    }
}