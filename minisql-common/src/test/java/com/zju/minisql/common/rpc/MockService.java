package com.zju.minisql.common.rpc;

public interface MockService {
    String ping(String message);
    int calculate(int a, int b);
}