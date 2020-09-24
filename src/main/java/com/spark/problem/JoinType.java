package com.spark.problem;

public enum JoinType {
    JOIN("join"),
    LEFT("left"),
    RIGHT("right"),
    OUTER("outer");

    private final String joinType;

    JoinType(String joinType) {
        this.joinType = joinType;
    }

    public String getJoinType() {
        return this.joinType;
    }
}
