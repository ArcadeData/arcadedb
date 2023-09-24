package com.arcadedb.query.nativ;

import com.arcadedb.database.Document;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface NativeRuntimeValue {

  Object eval(final Document record);
}
