package com.zju.minisql.common.service;

/**
 * 分布式 SQL 执行接口 (契约)
 * Master 仅依赖此接口，Worker 负责实现此接口
 */
public interface SqlExecuteService {
    
    /**
     * 接收并执行 SQL，返回执行结果
     */
    String execute(String sql);
}