package com.arcadedb.query.nativ;

import com.arcadedb.database.Document;
import com.arcadedb.serializer.json.JSONArray;

public class NativePropertyValue implements NativeRuntimeValue {
  public final String propertyName;

  public NativePropertyValue(final String propertyName) {
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
