package com.zju.minisql.client.parser;

import com.zju.minisql.common.query.model.AggregateCall;
import com.zju.minisql.common.query.model.AggregateFunction;
import com.zju.minisql.common.query.model.FilterCondition;
import com.zju.minisql.common.query.model.QueryAst;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于 JSqlParser 的 SQL 解析实现。
 * 当前阶段优先支持单表 SELECT-FROM-WHERE-GROUP BY 查询，以及双表等值 Join。
 */
public class JSqlParserSqlParser implements SqlParser {

    @Override
    public QueryAst parse(String sql) {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            if (!(statement instanceof Select)) {
                throw new IllegalArgumentException("当前仅支持 SELECT 查询");
            }
            PlainSelect plainSelect = (PlainSelect) ((Select) statement).getSelectBody();
            if (!(plainSelect.getFromItem() instanceof Table)) {
                throw new IllegalArgumentException("当前仅支持单表或双表 Join 查询");
            }

            QueryAst queryAst = new QueryAst();
            queryAst.setTableName(((Table) plainSelect.getFromItem()).getName());
            parseJoin(plainSelect, queryAst);
            parseSelectItems(plainSelect, queryAst);
            queryAst.setFilterCondition(parseWhere(plainSelect.getWhere()));
            parseGroupBy(plainSelect, queryAst);
            return queryAst;
        } catch (JSQLParserException e) {
            throw new IllegalArgumentException("SQL 解析失败: " + e.getMessage(), e);
        }
    }

    private void parseJoin(PlainSelect plainSelect, QueryAst queryAst) {
        List<Join> joins = plainSelect.getJoins();
        if (joins == null || joins.isEmpty()) {
            return;
        }
        if (joins.size() > 1) {
            throw new IllegalArgumentException("当前仅支持单个 Join");
        }

        Join join = joins.get(0);
        if (!(join.getRightItem() instanceof Table)) {
            throw new IllegalArgumentException("当前仅支持表与表之间的 Join");
        }
        if (!(join.getOnExpression() instanceof BinaryExpression)) {
            throw new IllegalArgumentException("当前仅支持等值 Join 条件");
        }

        BinaryExpression onExpression = (BinaryExpression) join.getOnExpression();
        if (!(onExpression.getLeftExpression() instanceof Column) || !(onExpression.getRightExpression() instanceof Column)) {
            throw new IllegalArgumentException("Join 条件两侧都必须是列名");
        }
        if (!"=".equals(onExpression.getStringExpression())) {
            throw new IllegalArgumentException("当前仅支持等值 Join");
        }

        queryAst.setJoinTableName(((Table) join.getRightItem()).getName());
        queryAst.setJoinLeftColumn(extractColumnRef((Column) onExpression.getLeftExpression()));
        queryAst.setJoinRightColumn(extractColumnRef((Column) onExpression.getRightExpression()));
    }

    private void parseSelectItems(PlainSelect plainSelect, QueryAst queryAst) {
        List<String> projectionColumns = new ArrayList<>();
        List<AggregateCall> aggregateCalls = new ArrayList<>();

        for (SelectItem selectItem : plainSelect.getSelectItems()) {
            if (selectItem instanceof AllColumns) {
                queryAst.setSelectAll(true);
                continue;
            }

            // 老版本 JSqlParser 必须强转为 SelectExpressionItem 才能获取表达式
            if (selectItem instanceof net.sf.jsqlparser.statement.select.SelectExpressionItem) {
                net.sf.jsqlparser.statement.select.SelectExpressionItem exprItem = 
                        (net.sf.jsqlparser.statement.select.SelectExpressionItem) selectItem;
                
                Expression expression = exprItem.getExpression();
                Alias alias = exprItem.getAlias();
                
                if (expression instanceof Column) {
                    projectionColumns.add(extractColumnRef((Column) expression));
                } else if (expression instanceof Function) {
                    aggregateCalls.add(parseAggregateCall((Function) expression, alias));
                } else {
                    throw new IllegalArgumentException("当前不支持的 SELECT 表达式: " + expression);
                }
            } else {
                throw new IllegalArgumentException("当前不支持的 SelectItem 类型: " + selectItem.getClass().getName());
            }
        }

        queryAst.setProjectionColumns(projectionColumns);
        queryAst.setAggregateCalls(aggregateCalls);
    }

    private AggregateCall parseAggregateCall(Function function, Alias alias) {
        String functionName = function.getName().toUpperCase();
        AggregateFunction aggregateFunction = AggregateFunction.valueOf(functionName);
        String columnName = "*";
        
        // 【修复点 2】：去掉了 <?>，并且使用 .getExpressions() 来安全获取参数列表
        ExpressionList parameters = function.getParameters();
        if (parameters != null && parameters.getExpressions() != null 
            && !parameters.getExpressions().isEmpty() 
            && parameters.getExpressions().get(0) instanceof Column) {
            columnName = extractColumnRef((Column) parameters.getExpressions().get(0));
        }
        
        String outputAlias = alias == null ? null : alias.getName();
        return new AggregateCall(aggregateFunction, columnName, outputAlias);
    }

    private FilterCondition parseWhere(Expression whereExpression) {
        if (whereExpression == null) {
            return null;
        }
        if (!(whereExpression instanceof BinaryExpression)) {
            throw new IllegalArgumentException("当前仅支持单个二元比较条件");
        }

        BinaryExpression binaryExpression = (BinaryExpression) whereExpression;
        if (!(binaryExpression.getLeftExpression() instanceof Column)) {
            throw new IllegalArgumentException("WHERE 左侧必须是列名");
        }

        String columnName = extractColumnRef((Column) binaryExpression.getLeftExpression());
        Object value = extractLiteralValue(binaryExpression.getRightExpression());
        FilterCondition.ComparisonOperator operator = parseOperator(binaryExpression.getStringExpression());
        return new FilterCondition(columnName, operator, value);
    }

    private void parseGroupBy(PlainSelect plainSelect, QueryAst queryAst) {
        List<String> groupByColumns = new ArrayList<>();
        if (plainSelect.getGroupBy() != null && plainSelect.getGroupBy().getGroupByExpressionList() != null) {
            for (Object expressionObject : plainSelect.getGroupBy().getGroupByExpressionList().getExpressions()) {
                Expression expression = (Expression) expressionObject;
                if (!(expression instanceof Column)) {
                    throw new IllegalArgumentException("当前仅支持按列进行 GROUP BY");
                }
                groupByColumns.add(extractColumnRef((Column) expression));
            }
        }
        queryAst.setGroupByColumns(groupByColumns);
    }

    private Object extractLiteralValue(Expression expression) {
        if (expression instanceof LongValue) {
            return ((LongValue) expression).getValue();
        }
        if (expression instanceof DoubleValue) {
            return ((DoubleValue) expression).getValue();
        }
        if (expression instanceof StringValue) {
            return ((StringValue) expression).getValue();
        }
        throw new IllegalArgumentException("当前仅支持数字和字符串字面量");
    }

    private FilterCondition.ComparisonOperator parseOperator(String operator) {
        switch (operator) {
            case "=":
                return FilterCondition.ComparisonOperator.EQ;
            case "<>":
            case "!=":
                return FilterCondition.ComparisonOperator.NE;
            case ">":
                return FilterCondition.ComparisonOperator.GT;
            case ">=":
                return FilterCondition.ComparisonOperator.GTE;
            case "<":
                return FilterCondition.ComparisonOperator.LT;
            case "<=":
                return FilterCondition.ComparisonOperator.LTE;
            default:
                throw new IllegalArgumentException("当前不支持的比较操作符: " + operator);
        }
    }

    private String extractColumnRef(Column column) {
        if (column.getTable() != null && column.getTable().getName() != null && !column.getTable().getName().isBlank()) {
            return column.getTable().getName() + "." + column.getColumnName();
        }
        return column.getColumnName();
    }
}