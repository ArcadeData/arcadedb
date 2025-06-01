package com.arcadedb.lucene.functions;

// Static import from ArcadeLuceneFunctionsUtils if getOrCreateMemoryIndex is public there, or keep local.
// For now, assuming it's accessible via ArcadeLuceneFunctionsUtils.
// import static com.arcadedb.lucene.functions.ArcadeLuceneFunctionsUtils.getOrCreateMemoryIndex;

import com.arcadedb.database.DatabaseContext; // Assuming CommandContext provides access to Database
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID; // Changed
import com.arcadedb.document.Document; // Changed
import com.arcadedb.document.Element; // Changed
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
import com.arcadedb.schema.DocumentType; // Changed
import com.arcadedb.schema.Schema; // Changed
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/** Created by frank on 15/01/2017. */
public class ArcadeLuceneSearchOnClassFunction extends ArcadeLuceneSearchFunctionTemplate { // Changed base class

  public static final String NAME = "search_class";

  public ArcadeLuceneSearchOnClassFunction() {
    super(NAME, 1, 2); // Original params: className, query, [metadata] - now query, [metadata] as class comes from context
                         // However, the original code takes classname as param for searchForIndex,
                         // but in execute it gets class from iThis.
                         // The original super was (NAME, 1, 2) -> (query, [metadata]), class was implicit from target.
                         // Let's stick to (NAME, 2, 3) -> (className, query, [metadata]) for now if it's a global function.
                         // If it's context aware (iThis), then (query, [metadata]) is fine.
                         // The original `search_class(<className>, <query>, [ <metadata> ])`
    // super(NAME, 2, 3); // (className, query, [metadata])
    // The original code for OLuceneSearchOnClassFunction used (NAME, 1, 2)
    // and derived className from `iThis` in `execute` or from `target` in `searchFromTarget`.
    // Let's keep the original arity and rely on context for class name.
     super(NAME, 1, 2);
  }

  @Override
  public String getName() {
    return NAME;
  }

  // canExecuteInline from template is likely fine if it relies on searchForIndex.

  @Override
  public Object execute( // FIXME: Signature might change
      Object iThis,
      Identifiable iCurrentRecord, // Changed
      Object iCurrentResult,
      Object[] params,
      CommandContext ctx) { // Changed

    Result result; // Changed
    if (iThis instanceof Result) {
      result = (Result) iThis;
    } else if (iThis instanceof Identifiable) {
      result = new ResultInternal((Identifiable) iThis); // Changed
    } else {
      // Cannot determine current record or class, perhaps throw error or return false
      return false;
    }

    if (!result.getElement().isPresent()) return false;
    Element element = result.getElement().get(); // Changed
    if (element.getType() == null) return false; // Changed, was getSchemaType().isPresent()

    String className = element.getType().getName(); // Changed

    ArcadeLuceneFullTextIndex index = searchForIndex(ctx, className); // FIXME

    if (index == null) return false;

    String query = (String) params[0];

    MemoryIndex memoryIndex = ArcadeLuceneFunctionsUtils.getOrCreateMemoryIndex(ctx);

    // FIXME: index.getDefinition() might be different.
    List<Object> key =
        index.getDefinition().getFields().stream()
            .map(s -> element.getProperty(s))
            .collect(Collectors.toList());

    // FIXME: index.buildDocument and index.indexAnalyzer might not exist or have different signatures
    org.apache.lucene.document.Document luceneDoc = index.buildDocument(key, iCurrentRecord);
    if (luceneDoc != null) {
       for (IndexableField field : luceneDoc.getFields()) {
        // Simplified, assuming stringValue is appropriate. Lucene's MemoryIndex.addField handles various IndexableField types.
        memoryIndex.addField(field.name(), field.stringValue(), index.indexAnalyzer());
      }
    }


    Document metadata = getMetadataDoc(params); // Changed
    // FIXME: LuceneCompositeKey and LuceneKeyAndMetadata need refactoring
    LuceneKeyAndMetadata keyAndMetadata =
        new LuceneKeyAndMetadata(
            new LuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata);

    // FIXME: index.buildQuery might not exist or have different signature
    return memoryIndex.search(index.buildQuery(keyAndMetadata)) > 0.0f;
  }

  private Document getMetadataDoc(Object[] params) { // Changed
    if (params.length == 2) { // Original used params[1] for metadata if arity was 2 (query, metadata)
      if (params[1] instanceof Map) {
        return new Document().fromMap((Map<String, ?>) params[1]); // Changed
      } else if (params[1] instanceof String) {
        return new Document().fromJSON((String) params[1]);
      }
      return new Document().fromJSON(params[1].toString());
    }
    // FIXME: LuceneQueryBuilder.EMPTY_METADATA
    return new Document(); //LuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  public String getSyntax() {
    // Original was "SEARCH_INDEX( indexName, [ metdatada {} ] )" which seems incorrect for search_class
    return "search_class( <query>, [ <metadata> ] )"; // Class is implicit from context
  }

  @Override
  public boolean filterResult() {
    return true;
  }

  // FIXME: This method's signature and logic are highly dependent on ArcadeDB's IndexableSQLFunction interface
  @Override
  public Iterable<Identifiable> searchFromTarget( // Changed
      FromClause target, // Changed
      BinaryCompareOperator operator, // Changed
      Object rightValue,
      CommandContext ctx, // Changed
      Expression... args) { // Changed

    // In this context, the class comes from the target FromClause
    ArcadeLuceneFullTextIndex index = searchForIndex(target, ctx, args); // FIXME

    Expression expression = args[0]; // Query is the first argument to the function
    String query = (String) expression.execute((Result) null, ctx); // Changed

    if (index != null) {
      Document meta = getMetadata(args, ctx, 1); // Metadata is the second argument (index 1) if present

      List<Identifiable> luceneResultSet; // Changed
      try (Stream<RID> rids = // Changed
          // FIXME: index.getInternal().getRids() needs to be replaced with ArcadeDB equivalent
          // This whole block is highly dependent on ArcadeLuceneFullTextIndex and LuceneKeyAndMetadata refactoring
          index
              .getAssociatedIndex() // Assuming getAssociatedIndex() is the way
              .getRids( // This method might not exist on ArcadeDB's Index interface
                  new LuceneKeyAndMetadata( // FIXME
                      new LuceneCompositeKey(Arrays.asList(query)).setContext(ctx), meta))) { // FIXME
        luceneResultSet = rids.collect(Collectors.toList());
      }
      return luceneResultSet;
    }
    return Collections.emptySet();
  }

  private Document getMetadata(Expression[] args, CommandContext ctx, int metadataParamIndex) { // Changed types
    if (args.length > metadataParamIndex) {
      return getMetadata(args[metadataParamIndex], ctx); // Calls method from ArcadeLuceneSearchFunctionTemplate
    }
    // FIXME: LuceneQueryBuilder.EMPTY_METADATA
    return new Document(); // LuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  protected ArcadeLuceneFullTextIndex searchForIndex( // Changed types
      FromClause target, CommandContext ctx, Expression... args) { // FIXME
    FromItem item = target.getItem(); // Changed

    // This function determines the class from the target (FROM clause)
    String className = item.getIdentifier().getStringValue(); // Changed

    return searchForIndex(ctx, className); // Calls private helper
  }

  private ArcadeLuceneFullTextIndex searchForIndex(CommandContext ctx, String className) { // Changed types
    DatabaseInternal database = (DatabaseInternal) ((DatabaseContext) ctx).getDatabase(); // FIXME: Verify
    // database.activateOnCurrentThread(); // May not be needed

    Schema schema = database.getSchema(); // Changed
    DocumentType docType = schema.getType(className); // Changed

    if (docType == null) {
      return null;
    }

    List<ArcadeLuceneFullTextIndex> indices = // Changed
        docType.getIndexes(true).stream() // getIndexes(true) for all indexes including supertypes
            .filter(idx -> idx instanceof ArcadeLuceneFullTextIndex) // FIXME
            .map(idx -> (ArcadeLuceneFullTextIndex) idx) // FIXME
            .collect(Collectors.toList());

    if (indices.size() > 1) {
      // Try to find an index that is defined ONLY on this class, not subclasses/supertypes if possible
      // Or, if multiple, pick one based on a convention (e.g. specific fields)
      // For now, this logic is simplified.
      // Original code just picked the first one if only one, or threw error.
      // We might need a more sophisticated way if multiple Lucene indexes can exist on a class hierarchy.
      for (ArcadeLuceneFullTextIndex idx : indices) {
          if (idx.getDefinition().getTypeName().equals(className)) { // Check if index is defined on this exact class
              return idx;
          }
      }
      // If no index is defined directly on this class, but inherited, it might be ambiguous.
      // However, the original code's filter `dbMetadata.getImmutableSchemaSnapshot().getClass(className).getIndexes()`
      // would only get indexes directly on that class.
      // `docType.getIndexes(true)` gets all. Let's refine to match original more closely for now:
      indices = docType.getIndexes(false).stream() // false = only indexes defined on this type
            .filter(idx -> idx instanceof ArcadeLuceneFullTextIndex) // FIXME
            .map(idx -> (ArcadeLuceneFullTextIndex) idx) // FIXME
            .collect(Collectors.toList());
       if (indices.size() > 1) {
           throw new IllegalArgumentException("Too many full-text indices on given class: " + className + ". Specify the index name using search_index function.");
       }
    }


    return indices.size() == 0 ? null : indices.get(0);
  }
}
