package com.zju.minisql.worker.storage.model;

import java.util.function.Predicate;

public class ScanOptions {

    private String startKey;
    private String endKey;
    private Predicate<Row> predicate;
    private int limit = -1;
    private int offset = 0;

    public String getStartKey() {
        return startKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
    }

    public Predicate<Row> getPredicate() {
        return predicate;
    }

    public void setPredicate(Predicate<Row> predicate) {
        this.predicate = predicate;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }
}
