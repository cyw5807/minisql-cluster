package com.zju.minisql.common.query.model;

import java.io.Serializable;
import java.util.LinkedHashMap;

/**
 * 一组 group key 对应的聚合桶。
 */
public class AggregateBucket implements Serializable {

    private LinkedHashMap<String, Object> groupValues = new LinkedHashMap<>();
    private LinkedHashMap<String, AggregateState> states = new LinkedHashMap<>();

    public LinkedHashMap<String, Object> getGroupValues() {
        return groupValues;
    }

    public void setGroupValues(LinkedHashMap<String, Object> groupValues) {
        this.groupValues = groupValues;
    }

    public LinkedHashMap<String, AggregateState> getStates() {
        return states;
    }

    public void setStates(LinkedHashMap<String, AggregateState> states) {
        this.states = states;
    }
}
