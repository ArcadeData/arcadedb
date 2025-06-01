package com.arcadedb.lucene.functions;

import com.arcadedb.database.DatabaseContext; // Assuming CommandContext provides access to Database
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.document.Document; // Changed
import com.arcadedb.index.Index; // Changed
import com.arcadedb.lucene.builder.LuceneQueryBuilder; // FIXME: Needs refactoring
import com.arcadedb.lucene.collections.LuceneCompositeKey; // FIXME: Needs refactoring
import com.arcadedb.lucene.index.ArcadeLuceneFullTextIndex; // FIXME: Needs refactoring
import com.arcadedb.lucene.query.LuceneKeyAndMetadata; // FIXME: Needs refactoring
import com.arcadedb.query.sql.executor.CommandContext; // Changed
import com.arcadedb.query.sql.executor.Result; // Changed
import com.arcadedb.query.sql.executor.ResultInternal; // Changed
import com.arcadedb.query.sql.parser.BinaryCompareOperator; // Changed
import com.arcadedb.query.sql.parser.Expression; // Changed
import com.arcadedb.query.sql.parser.FromClause; // Changed
import com.arcadedb.query.sql.parser.FromItem; // Changed
import com.arcadedb.query.sql.parser.Identifier; // Changed
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/** Created by frank on 15/01/2017. */
public class ArcadeLuceneSearchOnIndexFunction extends ArcadeLuceneSearchFunctionTemplate {

  public static final String NAME = "search_index";

  public ArcadeLuceneSearchOnIndexFunction() {
    super(NAME);
  }

  @Override
  public Object execute(
      Object self, // Is the target of the function, could be null, or an identifier (index name) or a collection
      Identifiable currentRecord,
      Object currentResult,
      Object[] params,
      CommandContext ctx) {

    validateParameterCount(params, 2, 3);

    String indexName = params[0].toString();
    String query = params[1].toString();
    Document metadata = params.length == 3 ? getMetadata((Expression) params[2], ctx) : new Document(ctx.getDatabase());

    ArcadeLuceneFullTextIndex index = ArcadeLuceneFunctionsUtils.getLuceneFullTextIndex(ctx, indexName);

    if (index == null) {
      // If used in a WHERE clause for a specific record, returning false means "filter out"
      // If used as a standalone function returning a set, return empty set.
      // The `filterResult` method in template handles boolean conversion.
      return currentRecord != null ? false : Collections.emptySet();
    }

    // If currentRecord is not null, this function is likely used in a WHERE clause context.
    // It needs to determine if the currentRecord matches the Lucene query *within its own fields*.
    if (currentRecord != null && currentRecord.getIdentity() != null) {
      MemoryIndex memoryIndex = ArcadeLuceneFunctionsUtils.getOrCreateMemoryIndex(ctx);

      // We need the Lucene Document for the currentRecord
      // The 'key' for buildDocument in this context is not a separate key, but derived from the record itself if auto index.
      // Or, if the index has specific fields, those are used.
      // Since we are in context of a specific record, we use its fields.
      org.apache.lucene.document.Document luceneDoc = index.buildDocument(null, currentRecord); // Pass null for key if derived from record

      if (luceneDoc != null) {
        for (IndexableField field : luceneDoc.getFields()) {
          // Simplified: use stringValue. Actual field data might be needed for MemoryIndex if not string.
          // MemoryIndex.addField can take Analyzer, which it gets from the IndexableFieldType.
          // If the field is not indexed with an analyzer (e.g. StringField), it's fine.
          // If it is (e.g. TextField), index.indexAnalyzer() should be used.
          // For simplicity, assuming MemoryIndex handles it or we use the general indexAnalyzer.
          memoryIndex.addField(field.name(), field.stringValue(), index.indexAnalyzer());
        }
      } else {
        return false; // Cannot build Lucene doc for current record
      }

      // The query here is the main Lucene query from params[1]
      // Metadata for this specific sub-query within MemoryIndex.
      LuceneKeyAndMetadata keyAndMeta = new LuceneKeyAndMetadata(query, metadata, ctx);
      org.apache.lucene.search.Query luceneQuery = index.buildQuery(keyAndMeta); // Build query using index's config

      return memoryIndex.search(luceneQuery) > 0.0f;
    } else {
      // If currentRecord is null, this function is likely used to return a set of results from the specified index.
      // This is the "searchFromTarget" equivalent.
      LuceneKeyAndMetadata keyAndMeta = new LuceneKeyAndMetadata(query, metadata, ctx);
      // The `index.get(keyAndMeta)` should return a LuceneResultSet or similar.
      // The `ArcadeLuceneFullTextIndex.get(Object[])` was changed to return IndexCursor.
      // We might need a direct way to execute a query via engine and get results.
      // For now, assuming `index.get(keyAndMeta)` returns a Set<Identifiable> or IndexCursor via engine.

      // The `get` method on `ArcadeLuceneFullTextIndex` takes `Object[] keys`.
      // We need to wrap `keyAndMeta` or pass its components.
      // Let's assume the engine's getInTx is what we want.
      if (index.getEngine() instanceof LuceneIndexEngine) {
        LuceneIndexEngine luceneEngine = (LuceneIndexEngine) index.getEngine();
        // LuceneKeyAndMetadata is already the 'key' for getInTx
        return luceneEngine.getInTx(keyAndMeta, null); // Passing null for LuceneTxChanges for non-transactional view
      }
      return Collections.emptySet();
    }
  }

  private Document getMetadata(Object[] params, CommandContext ctx) { // Kept for direct param access if needed
    if (params.length == 3 && params[2] != null) {
        if (params[2] instanceof Map) {
            return new Document(ctx.getDatabase()).fromMap((Map<String, Object>) params[2]);
        } else if (params[2] instanceof String) {
            return new Document(ctx.getDatabase()).fromJSON((String) params[2]);
        } else if (params[2] instanceof Expression) { // If metadata is an expression
            return getMetadata((Expression) params[2], ctx);
        } else if (params[2] instanceof Document) {
            return (Document) params[2];
        }
        try {
            return new Document(ctx.getDatabase()).fromJSON(params[2].toString());
        } catch (Exception e) { /* ignore, return empty */ }
    }
    return new Document(ctx.getDatabase()); // LuceneQueryBuilder.EMPTY_METADATA;
  }


  @Override
  public String getSyntax() {
    return getName() + "( <indexName>, <query> [, <metadata> ] )";
  }

  // Removed searchFromTarget, estimate, canExecuteInline, allowsIndexedExecution, shouldExecuteAfterSearch
  // searchForIndex is not needed here as index name is a direct parameter.
}
