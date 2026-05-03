package com.zju.minisql.worker.service;

import com.zju.minisql.common.service.SqlExecuteService;
import java.lang.management.ManagementFactory;

/**
 * 测试用的 SQL 执行实现类
 */
public class MockSqlExecuteServiceImpl implements SqlExecuteService {

    @Override
    public String execute(String sql) {
        // 获取当前进程的 PID，方便我们在控制台认出是哪个 Worker 在干活
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        
        System.out.println("==========================================");
        System.out.println("[Worker PID:" + pid + "] 收到 Master 派发的任务！");
        System.out.println("正在努力解析与执行 SQL: " + sql);
        System.out.println("==========================================\n");
        
        // 模拟执行耗时
        try { Thread.sleep(500); } catch (InterruptedException ignored) {}
        
        return "执行成功! 处理节点: Worker-" + pid;
    }
}