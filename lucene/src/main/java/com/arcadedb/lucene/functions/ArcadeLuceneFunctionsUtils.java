package com.arcadedb.lucene.functions;

import com.arcadedb.database.DatabaseContext; // Assuming CommandContext provides access to Database
import com.arcadedb.database.DatabaseInternal; // Changed
import com.arcadedb.index.Index; // Changed
import com.arcadedb.lucene.index.ArcadeLuceneFullTextIndex; // FIXME: Needs refactoring
import com.arcadedb.query.sql.executor.CommandContext; // Changed
import com.arcadedb.query.sql.executor.Result; // Changed
import com.arcadedb.query.sql.parser.Expression; // Changed
import com.arcadedb.schema.Schema; // Changed
import org.apache.lucene.index.memory.MemoryIndex;

/** Created by frank on 13/02/2017. */
public class ArcadeLuceneFunctionsUtils { // Changed class name
  public static final String MEMORY_INDEX = "_memoryIndex";

  protected static ArcadeLuceneFullTextIndex searchForIndex(Expression[] args, CommandContext ctx) { // Changed types
    final String indexName = (String) args[0].execute((Result) null, ctx); // Changed types
    return getLuceneFullTextIndex(ctx, indexName);
  }

  protected static ArcadeLuceneFullTextIndex getLuceneFullTextIndex( // Changed types
      final CommandContext ctx, final String indexName) {
    // Assuming CommandContext gives access to DatabaseInternal instance
    final DatabaseInternal database = (DatabaseInternal) ((DatabaseContext) ctx).getDatabase(); // FIXME: Verify how to get DatabaseInternal from CommandContext
    // database.activateOnCurrentThread(); // This might not be needed or done differently in ArcadeDB

    final Schema schema = database.getSchema(); // Changed OMetadataInternal

    // FIXME: metadata.getIndexManagerInternal().getIndex(documentDatabase, indexName) changed to schema.getIndex()
    // Also, the casting and type checking for ArcadeLuceneFullTextIndex needs ArcadeLuceneFullTextIndex to be properly defined and refactored.
    final Index index = schema.getIndex(indexName);

    if (!(index instanceof ArcadeLuceneFullTextIndex)) { // FIXME
      throw new IllegalArgumentException("Not a valid Lucene index:: " + indexName);
    }
    return (ArcadeLuceneFullTextIndex) index; // FIXME
  }

  public static MemoryIndex getOrCreateMemoryIndex(CommandContext ctx) { // Changed OCommandContext
    MemoryIndex memoryIndex = (MemoryIndex) ctx.getVariable(MEMORY_INDEX);
    if (memoryIndex == null) {
      memoryIndex = new MemoryIndex();
      ctx.setVariable(MEMORY_INDEX, memoryIndex);
    }
    memoryIndex.reset();
    return memoryIndex;
  }

  public static String doubleEscape(final String s) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < s.length(); ++i) {
      final char c = s.charAt(i);
      if (c == 92 || c == 43 || c == 45 || c == 33 || c == 40 || c == 41 || c == 58 || c == 94
          || c == 91 || c == 93 || c == 34 || c == 123 || c == 125 || c == 126 || c == 42 || c == 63
          || c == 124 || c == 38 || c == 47) {
        sb.append('\\');
        sb.append('\\');
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
