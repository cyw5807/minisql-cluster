package com.zju.minisql.worker.storage;

import com.zju.minisql.worker.storage.model.OpType;
import com.zju.minisql.worker.storage.model.ReplicationEntry;
import com.zju.minisql.worker.storage.model.Row;
import com.zju.minisql.worker.storage.model.ScanOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class LocalStorageEngineImplTest {

    @TempDir
    Path tempDir;

    /**
     * 场景 T1：基本写入 / 读取
     * 向引擎插入一行数据（student 表，主键 "1001"），
     * 验证 get 按主键能精确返回该行，且 scan 在无任何过滤条件下能遍历到该行。
     */
    @Test
    void shouldInsertGetAndScan() {
        LocalStorageEngine engine = new LocalStorageEngineImpl(tempDir);
        Row row = new Row("1001", "student", 0, new HashMap<>());
        row.getColumns().put("name", "Alice");

        engine.insert("student", row);
        Row loaded = engine.get("student", "1001");
        Assertions.assertNotNull(loaded);
        Assertions.assertEquals("Alice", loaded.getColumns().get("name"));

        ScanOptions opts = new ScanOptions();
        Iterator<Row> iterator = engine.scan("student", opts);
        Assertions.assertTrue(iterator.hasNext());
    }

    /**
     * 场景 T2：范围扫描 + 谓词下推 + 分页
     * 插入 k1~k4 四行（score 分别为 8/12/30/16），
     * 使用 [k2, k4) 范围 + score>10 谓词 + offset=1 / limit=1 进行分页扫描。
     * 期望：满足条件的行为 k2(12)、k3(30)、k4(16)，去掉 offset=1 后只剩 k3，
     * 验证仅返回主键 "k3" 这一行，且迭代器无后续元素。
     */
    @Test
    void shouldApplyRangePredicateAndPaginationWhenScan() {
        LocalStorageEngine engine = new LocalStorageEngineImpl(tempDir);
        engine.insert("student", row("k1", 0, 8));
        engine.insert("student", row("k2", 0, 12));
        engine.insert("student", row("k3", 0, 30));
        engine.insert("student", row("k4", 0, 16));

        ScanOptions opts = new ScanOptions();
        opts.setStartKey("k2");
        opts.setEndKey("k4");
        opts.setPredicate(r -> (Integer) r.getColumns().get("score") > 10);
        opts.setOffset(1);
        opts.setLimit(1);

        Iterator<Row> iterator = engine.scan("student", opts);
        Assertions.assertTrue(iterator.hasNext());
        Row only = iterator.next();
        Assertions.assertEquals("k3", only.getPrimaryKey());
        Assertions.assertFalse(iterator.hasNext());
    }

    /**
     * 场景 T3：副本日志回放（INSERT / UPDATE / DELETE）
     * 通过 applyReplicationLog 接口顺序回放三条操作日志：
     * 1. INSERT logIndex=1 → 写入 score=10
     * 2. UPDATE logIndex=2 → score 更新为 99
     * 3. DELETE logIndex=3 → 行被删除
     * 验证每步执行后存储状态与期望一致，最终 get 返回 null。
     */
    @Test
    void shouldApplyReplicationLogForInsertUpdateAndDelete() {
        LocalStorageEngine engine = new LocalStorageEngineImpl(tempDir);
        Row inserted = row("k1", 0, 10);
        engine.applyReplicationLog(new ReplicationEntry(OpType.INSERT, "student", 0, inserted, "k1", 1L, 1L));
        Assertions.assertEquals(10, engine.get("student", "k1").getColumns().get("score"));

        Row updated = row("k1", 0, 99);
        engine.applyReplicationLog(new ReplicationEntry(OpType.UPDATE, "student", 0, updated, "k1", 2L, 2L));
        Assertions.assertEquals(99, engine.get("student", "k1").getColumns().get("score"));

        engine.applyReplicationLog(new ReplicationEntry(OpType.DELETE, "student", 0, updated, "k1", 3L, 3L));
        Assertions.assertNull(engine.get("student", "k1"));
    }

    @Test
    void shouldDeleteAllRowsWhenDropTable() {
        LocalStorageEngine engine = new LocalStorageEngineImpl(tempDir);
        engine.insert("student", row("k1", 0, 10));
        engine.insert("student", row("k2", 1, 11));
        engine.insert("course", row("c1", 0, 99));

        engine.deleteTable("student");

        Assertions.assertNull(engine.get("student", "k1"));
        Assertions.assertNull(engine.get("student", "k2"));
        Assertions.assertNotNull(engine.get("course", "c1"));
    }

    /**
     * 场景 T4：分片导出 / 导入
     * 在源引擎 partition=0 插入一行（score=88），
     * 通过 exportPartition 序列化为字节数组，
     * 再通过 importPartition 写入目标引擎的 partition=3（不同分区 ID）。
     * 验证目标引擎能按主键读到相同数据，证明跨节点分片迁移时数据不丢失。
     */
    @Test
    void shouldExportAndImportPartitionData() {
        LocalStorageEngine source = new LocalStorageEngineImpl(tempDir.resolve("source"));
        LocalStorageEngine target = new LocalStorageEngineImpl(tempDir.resolve("target"));
        source.insert("student", row("k8", 0, 88));

        byte[] data = source.exportPartition(0);
        target.importPartition(3, data);

        Row loaded = target.get("student", "k8");
        Assertions.assertNotNull(loaded);
        Assertions.assertEquals(88, loaded.getColumns().get("score"));
    }

    private Row row(String key, int partitionId, int score) {
        Row row = new Row(key, "student", partitionId, new HashMap<>());
        Map<String, Object> columns = row.getColumns();
        columns.put("score", score);
        return row;
    }
}
