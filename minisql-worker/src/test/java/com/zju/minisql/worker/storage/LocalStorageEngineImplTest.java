package com.zju.minisql.worker.storage;

import com.zju.minisql.worker.storage.model.Row;
import com.zju.minisql.worker.storage.model.ScanOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;

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
}
