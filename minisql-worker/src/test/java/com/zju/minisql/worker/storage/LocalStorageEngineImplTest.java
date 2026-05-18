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
