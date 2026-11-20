package com.arcadedb.opengrep;

import com.arcadedb.serializer.json.JSONArray;

// ruleid: claude-named-as-author
// @author Claude

// ok: fully-qualified-arcadedb-type
/** Builds a {@link com.arcadedb.serializer.json.JSONArray}. */
class QualifiedNames {
  void qualifiedException() {
    // ruleid: fully-qualified-arcadedb-type
    throw new com.arcadedb.exception.CommandSQLParsingException("x");
  }

  Object qualifiedAnonymousClass() {
    // ruleid: fully-qualified-arcadedb-type
    return new com.arcadedb.query.opencypher.ast.Expression() {
    };
  }

  Object imported(final Object files) {
    // ok: fully-qualified-arcadedb-type
    return new JSONArray(files);
  }
}
