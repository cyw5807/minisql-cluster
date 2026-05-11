package com.zju.minisql.worker.storage;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 分片文件管理。
 */
public class PartitionManager {

    private final Path dataDir;
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, byte[]>> storeMap = new ConcurrentHashMap<>();

    public PartitionManager(Path dataDir) {
        this.dataDir = dataDir;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new RuntimeException("创建数据目录失败: " + dataDir, e);
        }
    }

    public Map<String, byte[]> getStore(int partitionId) {
        return storeMap.computeIfAbsent(partitionId, this::loadOrCreate);
    }

    public void flush(int partitionId) {
        ConcurrentHashMap<String, byte[]> partition = storeMap.get(partitionId);
        if (partition == null) {
            return;
        }
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(partition);
            oos.close();
            Files.write(filePath(partitionId), bos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("落盘失败, partition=" + partitionId, e);
        }
    }

    public byte[] exportPartition(int partitionId) {
        try {
            flush(partitionId);
            return Files.readAllBytes(filePath(partitionId));
        } catch (IOException e) {
            throw new RuntimeException("导出分片失败, partition=" + partitionId, e);
        }
    }

    public void importPartition(int partitionId, byte[] data) {
        try {
            Files.write(filePath(partitionId), data);
            storeMap.put(partitionId, deserialize(data));
        } catch (IOException e) {
            throw new RuntimeException("导入分片失败, partition=" + partitionId, e);
        }
    }

    public Map<Integer, ConcurrentHashMap<String, byte[]>> stores() {
        return storeMap;
    }

    private ConcurrentHashMap<String, byte[]> loadOrCreate(int partitionId) {
        Path path = filePath(partitionId);
        if (!Files.exists(path)) {
            return new ConcurrentHashMap<>();
        }
        try {
            return deserialize(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new RuntimeException("读取分片失败, partition=" + partitionId, e);
        }
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<String, byte[]> deserialize(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            Object read = in.readObject();
            if (read instanceof ConcurrentHashMap) {
                return (ConcurrentHashMap<String, byte[]>) read;
            }
            return new ConcurrentHashMap<>();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("反序列化分片失败", e);
        }
    }

    private Path filePath(int partitionId) {
        return dataDir.resolve("partition-" + partitionId + ".bin");
    }
}
