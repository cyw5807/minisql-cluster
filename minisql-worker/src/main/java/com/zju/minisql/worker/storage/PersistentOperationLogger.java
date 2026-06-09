package com.zju.minisql.worker.storage;

import com.zju.minisql.worker.storage.model.ReplicationEntry;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Worker 本地持久化操作日志。
 */
public class PersistentOperationLogger {

    private static final String CRUD_LOG = "operation-crud.log";
    private static final String ENTRY_LOG = "replication-entry.log";

    private final Path crudLogPath;
    private final Path entryLogPath;
    private final Object lock = new Object();

    public PersistentOperationLogger(Path dataDir) {
        try {
            Path logDir = dataDir.resolve("logs");
            Files.createDirectories(logDir);
            this.crudLogPath = logDir.resolve(CRUD_LOG);
            this.entryLogPath = logDir.resolve(ENTRY_LOG);
        } catch (IOException e) {
            throw new RuntimeException("初始化持久化日志目录失败: " + dataDir, e);
        }
    }

    public void logCrud(String op, String tableName, String primaryKey, Integer partitionId, String detail) {
        long now = System.currentTimeMillis();
        String line = "{"
                + "\"ts\":" + now + ","
                + "\"op\":\"" + escape(op) + "\","
                + "\"table\":\"" + escape(tableName) + "\","
                + "\"pk\":\"" + escape(primaryKey) + "\","
                + "\"partition\":" + (partitionId == null ? "null" : partitionId) + ","
                + "\"detail\":\"" + escape(detail) + "\""
                + "}";
        appendLine(crudLogPath, line);
    }

    public void logEntry(String source, ReplicationEntry entry) {
        long now = System.currentTimeMillis();
        String line = "{"
                + "\"ts\":" + now + ","
                + "\"source\":\"" + escape(source) + "\","
                + "\"type\":\"" + (entry.getType() == null ? "" : escape(entry.getType().name())) + "\","
                + "\"table\":\"" + escape(entry.getTableName()) + "\","
                + "\"pk\":\"" + escape(entry.getPrimaryKey()) + "\","
                + "\"partition\":" + entry.getPartitionId() + ","
                + "\"logIndex\":" + entry.getLogIndex() + ","
                + "\"entryTs\":" + entry.getTimestamp()
                + "}";
        appendLine(entryLogPath, line);
    }

    public Path getCrudLogPath() {
        return crudLogPath;
    }

    public Path getEntryLogPath() {
        return entryLogPath;
    }

    private void appendLine(Path target, String line) {
        synchronized (lock) {
            try {
                Files.writeString(
                        target,
                        line + System.lineSeparator(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                );
            } catch (IOException e) {
                throw new RuntimeException("写入日志失败: " + target, e);
            }
        }
    }

    private String escape(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}
