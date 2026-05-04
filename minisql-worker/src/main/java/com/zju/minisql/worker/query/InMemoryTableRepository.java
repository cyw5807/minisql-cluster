package com.zju.minisql.worker.query;

import com.zju.minisql.common.query.model.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 课程作业阶段的内存表仓库。
 * 在组员 A 的真实存储引擎接入前，先用它打通查询执行链路。
 */
public class InMemoryTableRepository {

    private final Map<String, List<Row>> tables = new HashMap<>();

    public void putTable(String tableName, List<Row> rows) {
        tables.put(tableName.toLowerCase(), rows);
    }

    public List<Row> getTableRows(String tableName) {
        return tables.getOrDefault(tableName.toLowerCase(), List.of());
    }

    public static InMemoryTableRepository demoRepositoryFor(String workerAddress) {
        InMemoryTableRepository repository = new InMemoryTableRepository();
        List<Row> allStudents = List.of(
                Row.of("id", 1001L, "name", "Alice", "dept", "CS", "score", 95L),
                Row.of("id", 1002L, "name", "Bob", "dept", "EE", "score", 88L),
                Row.of("id", 1003L, "name", "Carol", "dept", "CS", "score", 91L),
                Row.of("id", 1004L, "name", "David", "dept", "ME", "score", 84L),
                Row.of("id", 1005L, "name", "Eve", "dept", "EE", "score", 97L)
        );
        List<Row> allScores = List.of(
                Row.of("id", 1001L, "course", "DistributedDB", "grade", 95L),
                Row.of("id", 1002L, "course", "ComputerNetwork", "grade", 88L),
                Row.of("id", 1003L, "course", "OperatingSystem", "grade", 91L),
                Row.of("id", 1004L, "course", "MechanicalDesign", "grade", 84L),
                Row.of("id", 1005L, "course", "PowerSystem", "grade", 97L)
        );

        int bucketIndex = workerAddress.endsWith(":9011") ? 0 : 1;
        repository.putTable("student", partitionById(allStudents, bucketIndex, 2));
        repository.putTable("score", partitionById(allScores, bucketIndex, 2));
        return repository;
    }

    private static List<Row> partitionById(List<Row> rows, int bucketIndex, int bucketCount) {
        List<Row> localRows = new ArrayList<>();
        for (Row row : rows) {
            long id = ((Number) row.get("id")).longValue();
            int targetIndex = Math.floorMod(String.valueOf(id).hashCode(), bucketCount);
            if (targetIndex == bucketIndex) {
                localRows.add(row);
            }
        }
        return localRows;
    }
}
