package com.zju.minisql.worker.storage;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;

/**
 * 分片文件管理。
 */
public class PartitionManager {

    private final Path dataDir;
    private final ConcurrentHashMap<Integer, MVStore> storeMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, MVMap<String, byte[]>> partitionDataMap = new ConcurrentHashMap<>();
    private static final Pattern FILE_PATTERN = Pattern.compile("partition-(\\d+)\\.db");
    private static final String DATA_MAP = "rows";

    public PartitionManager(Path dataDir) {
        this.dataDir = dataDir;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new RuntimeException("创建数据目录失败: " + dataDir, e);
        }
    }

    public Map<String, byte[]> getStore(int partitionId) {
        return partitionDataMap.computeIfAbsent(partitionId, id -> openStore(id).openMap(DATA_MAP));
    }

    public void flush(int partitionId) {
        MVStore store = storeMap.get(partitionId);
        if (store == null) {
            return;
        }
        store.commit();
    }

    public byte[] exportPartition(int partitionId) {
        try {
            getStore(partitionId);
            flush(partitionId);
            closeStore(partitionId);
            byte[] bytes = Files.readAllBytes(filePath(partitionId));
            getStore(partitionId);
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException("导出分片失败, partition=" + partitionId, e);
        }
    }

    public void importPartition(int partitionId, byte[] data) {
        try {
            closeStore(partitionId);
            Files.write(filePath(partitionId), data);
            getStore(partitionId);
        } catch (IOException e) {
            throw new RuntimeException("导入分片失败, partition=" + partitionId, e);
        }
    }

    public Map<Integer, Map<String, byte[]>> stores() {
        Set<Integer> partitionIds = detectPartitions();
        Map<Integer, Map<String, byte[]>> stores = new HashMap<>();
        for (Integer partitionId : partitionIds) {
            stores.put(partitionId, getStore(partitionId));
        }
        return stores;
    }

    private MVStore openStore(int partitionId) {
        return storeMap.computeIfAbsent(partitionId, id -> MVStore.open(filePath(id).toString()));
    }

    private Set<Integer> detectPartitions() {
        Set<Integer> ids = new HashSet<>(partitionDataMap.keySet());
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "partition-*.db")) {
            for (Path path : stream) {
                Matcher matcher = FILE_PATTERN.matcher(path.getFileName().toString());
                if (matcher.matches()) {
                    ids.add(Integer.parseInt(matcher.group(1)));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("扫描分片文件失败: " + dataDir, e);
        }
        return ids;
    }

    private void closeStore(int partitionId) {
        MVStore store = storeMap.remove(partitionId);
        partitionDataMap.remove(partitionId);
        if (store != null && !store.isClosed()) {
            store.close();
        }
    }

    private Path filePath(int partitionId) {
        return dataDir.resolve("partition-" + partitionId + ".db");
    }
}
