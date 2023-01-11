package com.arcadedb.query.sql.executor;

public class IndexSearchInfo {
  private boolean        allowsRangeQueries;
  private boolean        map;
  private boolean        indexByKey;
  private String         field;
  private CommandContext ctx;
  private boolean        indexByValue;

  public IndexSearchInfo(String indexField, boolean allowsRangeQueries, boolean map, boolean indexByKey, boolean indexByValue, CommandContext ctx) {
    this.field = indexField;
    this.allowsRangeQueries = allowsRangeQueries;
    this.map = map;
    this.indexByKey = indexByKey;
    this.ctx = ctx;
    this.indexByValue = indexByValue;
  }

  public String getField() {
    return field;
  }

  public CommandContext getCtx() {
    return ctx;
  }

  public boolean allowsRange() {
    return allowsRangeQueries;
  }

  public boolean isMap() {
    return map;
  }

  public boolean isIndexByKey() {
    return indexByKey;
  }

  public boolean isIndexByValue() {
    return indexByValue;
  }
}
