package com.zju.minisql.worker.query;

import com.zju.minisql.common.query.model.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 工业级弹性的内存表仓库。
 * 【特性】：
 * 1. 践行控制流与数据流分离，作为纯粹的弹性数据桶，不硬编码任何拓扑人数（如3个桶），天然支持节点动态下线。
 * 2. 彻底移除预加载的 Mock 数据，转为完全由后续运行期 INSERT 语句动态写入。
 */
public class InMemoryTableRepository implements WorkerTableRepository {

    // 使用 ConcurrentHashMap 确保高并发 RPC 读写时的线程安全
    private final Map<String, List<Row>> tables = new ConcurrentHashMap<>();

    /**
     * 覆盖或初始化整张表的数据（常用于批量同步）
     */
    public void putTable(String tableName, List<Row> rows) {
        tables.put(tableName.toLowerCase(), new ArrayList<>(rows));
    }

    /**
     * 获取当前 Worker 节点本地存储的局部数据分片
     */
    @Override
    public List<Row> getTableRows(String tableName) {
        return tables.getOrDefault(tableName.toLowerCase(), List.of());
    }

    /**
     * ⭐ 【核心新增】：单条数据动态插入能力！
     * 用于支撑后续演示过程中，用户在 CLI 界面亲手敲入的 INSERT INTO 语句。
     * 无论集群是3个节点还是因为下线变成了2个节点，Client 路由发过来的数据都会被该方法稳稳接住。
     */
    @Override
    public synchronized void insertRow(String tableName, Row row) {
        String key = tableName.toLowerCase();
        // 如果表不存在（比如刚执行完 CREATE TABLE），则初始化一个空列表并追加数据
        tables.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
    }

    @Override
    public void deleteTable(String tableName) {
        tables.remove(tableName.toLowerCase());
    }

    /**
     * ⭐ 【框架装配方法】：返回一个纯净、空白的本地仓库
     * 遵循组长指示：不提前注入任何测试数据，还原本质，静待后续展示过程中的物理写入。
     */
    public static InMemoryTableRepository demoRepositoryFor(String workerAddress) {
        System.out.println("📦 存储节点 [" + workerAddress + "] 弹性数据仓库初始化成功，当前状态：[纯净空仓]，等待数据流接入...");
        return new InMemoryTableRepository();
    }
}