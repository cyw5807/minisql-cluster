package com.zju.minisql.master.meta;

import com.zju.minisql.common.meta.ColumnMeta;
import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.common.rpc.serialize.KryoSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 元数据管理器集成测试 (依赖本地 ZK 运行)
 */
public class MetadataManagerTest {

    private static CuratorFramework zkClient;
    private static MetadataManager metadataManager;
    private static final String TEST_TABLE = "users_test_table";

    @BeforeAll
    public static void setUp() throws Exception {
        // 连接到本地 ZK
        zkClient = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        
        // 实例化组件
        metadataManager = new MetadataManager(zkClient, new KryoSerializer());
        metadataManager.init();

        // 预防性清理：如果上次测试残留了该表，先删掉
        try {
            metadataManager.dropTable(TEST_TABLE);
        } catch (Exception ignored) {}
    }

    @Test
    public void testTableCrudOperations() throws Exception {
        // 1. 构造一张测试表
        TableMeta table = new TableMeta(TEST_TABLE);
        table.addColumn(new ColumnMeta("id", "INT", 0, true, true));
        table.addColumn(new ColumnMeta("username", "VARCHAR", 50, false, false));

        // 2. 测试 CREATE
        assertDoesNotThrow(() -> metadataManager.createTable(table));

        // 3. 测试 SHOW TABLES
        List<String> tables = metadataManager.getAllTableNames();
        assertTrue(tables.contains(TEST_TABLE), "获取所有表名列表中应该包含刚创建的测试表");

        // 4. 测试 SELECT (反序列化)
        TableMeta fetchedTable = metadataManager.getTable(TEST_TABLE);
        assertNotNull(fetchedTable);
        assertEquals(TEST_TABLE, fetchedTable.getTableName());
        assertEquals(2, fetchedTable.getColumns().size());
        assertEquals("id", fetchedTable.getPrimaryKey().getColumnName());

        // 5. 测试 DROP
        assertDoesNotThrow(() -> metadataManager.dropTable(TEST_TABLE));
        assertNull(metadataManager.getTable(TEST_TABLE), "删除后查询应该返回 null");
    }

    @AfterAll
    public static void tearDown() {
        if (zkClient != null) {
            zkClient.close();
        }
    }
}