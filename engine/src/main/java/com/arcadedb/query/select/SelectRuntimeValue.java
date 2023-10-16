package com.arcadedb.query.select;

import com.arcadedb.database.Document;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface SelectRuntimeValue {

  Object eval(final Document record);
}
