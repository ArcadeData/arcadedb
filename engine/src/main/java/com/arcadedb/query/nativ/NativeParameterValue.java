package com.arcadedb.query.nativ;

import com.arcadedb.database.Document;

public class NativeParameterValue implements NativeRuntimeValue {
  public final  String       parameterName;
  private final NativeSelect select;

  public NativeParameterValue(final NativeSelect select, final String parameterName) {
    this.select = select;
    this.parameterName = parameterName;
  }

  @Override
  public Object eval(final Document record) {
    if (select.parameters == null)
      throw new IllegalArgumentException("Missing parameter '" + parameterName + "'");
    if (!select.parameters.containsKey(parameterName))
      throw new IllegalArgumentException("Missing parameter '" + parameterName + "'");
    return select.parameters.get(parameterName);
  }

  @Override
  public String toString() {
    return "#" + parameterName;
  }
}
