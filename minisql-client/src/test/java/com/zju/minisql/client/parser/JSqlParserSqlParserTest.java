package com.zju.minisql.client.parser;

import com.zju.minisql.common.query.model.AggregateFunction;
import com.zju.minisql.common.query.model.QueryAst;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SQL 解析器测试。
 */
public class JSqlParserSqlParserTest {

    private final JSqlParserSqlParser sqlParser = new JSqlParserSqlParser();

    @Test
    public void testParseAggregateQuery() {
        QueryAst ast = sqlParser.parse("SELECT dept, COUNT(*) AS cnt FROM student WHERE score >= 90 GROUP BY dept");
        assertEquals("student", ast.getTableName());
        assertEquals("score", ast.getFilterCondition().getColumnName());
        assertEquals("dept", ast.getProjectionColumns().get(0));
        assertEquals(AggregateFunction.COUNT, ast.getAggregateCalls().get(0).getFunction());
        assertEquals("cnt", ast.getAggregateCalls().get(0).getOutputName());
        assertTrue(ast.hasAggregation());
    }

    @Test
    public void testParseJoinQuery() {
        QueryAst ast = sqlParser.parse("SELECT student.name, score.course FROM student JOIN score ON student.id = score.id");
        assertEquals("student", ast.getTableName());
        assertEquals("score", ast.getJoinTableName());
        assertEquals("student.id", ast.getJoinLeftColumn());
        assertEquals("score.id", ast.getJoinRightColumn());
        assertTrue(ast.hasJoin());
    }
}
