package com.arcadedb.query.select;

import com.arcadedb.database.Document;

public class SelectPropertyValue implements SelectRuntimeValue {
  public final String propertyName;

  public SelectPropertyValue(final String propertyName) {
    this.propertyName = propertyName;
  }

  @Override
  public Object eval(final Document record) {
    return record.get(propertyName);
  }

  @Override
  public String toString() {
    return ":" + propertyName;
  }
}
