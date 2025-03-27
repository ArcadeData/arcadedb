package com.arcadedb.query.sql.executor;

public class IndexSearchInfo {
  private final boolean        allowsRangeQueries;
  private final boolean        map;
  private final boolean        indexByKey;
  private final String         field;
  private final CommandContext context;
  private final boolean        indexByValue;
  private final boolean        supportNull;

  public IndexSearchInfo(final String indexField, final boolean allowsRangeQueries, final boolean map, final boolean indexByKey,
      final boolean indexByValue,
      final boolean supportNull, final CommandContext context) {
    this.field = indexField;
    this.allowsRangeQueries = allowsRangeQueries;
    this.map = map;
    this.indexByKey = indexByKey;
    this.context = context;
    this.indexByValue = indexByValue;
    this.supportNull = supportNull;
  }

  public String getField() {
    return field;
  }

  public CommandContext getContext() {
    return context;
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

  public boolean isSupportNull() {
    return supportNull;
  }
}
