package com.zju.minisql.client.meta;

import com.zju.minisql.common.meta.ColumnMeta;
import com.zju.minisql.common.meta.MetadataManager;
import com.zju.minisql.common.meta.TableMeta;
import com.zju.minisql.common.rpc.serialize.KryoSerializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 元数据管理器集成测试 (使用内存 ZK 运行，彻底隔离物理环境)
 */
public class MetadataManagerTest {

    private static TestingServer zkTestServer;
    private static CuratorFramework zkClient;
    private static MetadataManager metadataManager;
    private static final String TEST_TABLE = "users_test_table";

    @BeforeAll
    public static void setUp() throws Exception {
        // 1. 启动一个完全在内存中运行的临时 ZK 服务端
        zkTestServer = new TestingServer();
        zkTestServer.start();

        // 2. 连接到内存 ZK (动态获取 zkTestServer 随机分配的可用端口)
        zkClient = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new ExponentialBackoffRetry(1000, 3));
        zkClient.start();
        
        // 3. 实例化组件
        metadataManager = new MetadataManager(zkClient, new KryoSerializer());
        metadataManager.init();

        // 预防性清理 (因为是内存 ZK，其实这步可以省略，但保留以防以后复用代码)
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
    public static void tearDown() throws Exception {
        // 优雅关闭客户端
        if (zkClient != null) {
            zkClient.close();
        }
        // 测试结束，销毁内存 ZK，绝不残留脏数据或占用端口
        if (zkTestServer != null) {
            zkTestServer.stop();
        }
    }
}