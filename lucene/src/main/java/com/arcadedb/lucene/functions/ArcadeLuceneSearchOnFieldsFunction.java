package com.arcadedb.lucene.functions;

// import static com.arcadedb.lucene.functions.ArcadeLuceneFunctionsUtils.getOrCreateMemoryIndex; // Assuming public access

import com.arcadedb.database.DatabaseContext; // Assuming CommandContext provides access to Database
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID; // Changed
import com.arcadedb.document.Document; // Changed
import com.arcadedb.document.Element; // Changed
import com.arcadedb.index.Index; // Changed
import com.arcadedb.index.IndexDefinition;
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
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType; // Changed
import com.arcadedb.schema.Schema; // Changed
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/** Created by frank on 15/01/2017. */
public class ArcadeLuceneSearchOnFieldsFunction extends ArcadeLuceneSearchFunctionTemplate { // Changed base class

  public static final String NAME = "search_fields";

  public ArcadeLuceneSearchOnFieldsFunction() {
    // Original params: fieldNames, query, [metadata]
    // Class name is derived from context (iThis or target)
    super(NAME, 2, 3);
  }

  @Override
  public String getName() {
    return NAME;
  }

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
      return false; // Cannot determine current record
    }

    if (!result.getElement().isPresent()) return false;
    Element element = result.getElement().get(); // Changed
    if (element.getType() == null) return false; // Changed
    String className = element.getType().getName(); // Changed

    @SuppressWarnings("unchecked")
    List<String> fieldNames = (List<String>) params[0];

    // Note: searchForIndex here might not be strictly necessary if we always build an in-memory index from the current record's fields.
    // However, the original code uses it to get definition and analyzer.
    ArcadeLuceneFullTextIndex index = searchForIndex(className, ctx, fieldNames); // FIXME

    if (index == null) {
        // If no pre-existing index matches, we might still proceed if we can get a default analyzer
        // or one from metadata, but building a Lucene document without an IndexDefinition is problematic.
        // For now, returning false if no suitable index is found to provide an analyzer/definition.
        // This part might need a different strategy for on-the-fly indexing without a backing index.
      return false;
    }

    String query;
    if (params.length < 2 || params[1] == null) { // query is params[1]
      query = null;
    } else {
      query = params[1].toString();
    }

    MemoryIndex memoryIndex = ArcadeLuceneFunctionsUtils.getOrCreateMemoryIndex(ctx);

    // FIXME: This part needs to build a Lucene document using ONLY the specified fieldNames
    // from the 'element', and using the types from the schema for those fields.
    // The 'key' concept from OLuceneSearchOnIndexFunction is not directly applicable here in the same way.
    // index.buildDocument(key, iCurrentRecord) is not right for this context.
    org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();
    DocumentType docType = element.getType();
    if (docType != null) {
        for(String fieldName : fieldNames) {
            if (element.has(fieldName)) {
                Object fieldValue = element.getProperty(fieldName);
                com.arcadedb.schema.Property prop = docType.getProperty(fieldName);
                Type fieldType = prop != null ? prop.getType() : Type.STRING; // Default to string if no prop
                // FIXME: ArcadeLuceneIndexType.createFields needs correct store/sort parameters.
                // Assuming Field.Store.YES and no sorting for memory index fields for now.
                List<org.apache.lucene.document.Field> fields = ArcadeLuceneIndexType.createFields(fieldName, fieldValue, Field.Store.YES, false, fieldType);
                for(org.apache.lucene.document.Field f : fields) {
                    luceneDoc.add(f);
                }
            }
        }
    }

    if (luceneDoc.getFields().isEmpty()) return false; // No fields were added

    // Add all fields from the created luceneDoc to memoryIndex
    for (IndexableField field : luceneDoc.getFields()) {
       // Simplified, assuming stringValue is appropriate for all, which is not robust.
       // MemoryIndex.addField handles various IndexableField types, so this might be okay if createFields returns typed fields.
       memoryIndex.addField(field.name(), field.stringValue(), index.indexAnalyzer()); // FIXME: index.indexAnalyzer() dependency
    }


    Document metadata = getMetadataDoc(params, 2); // metadata is params[2]
    // FIXME: LuceneCompositeKey and LuceneKeyAndMetadata need refactoring
    LuceneKeyAndMetadata keyAndMetadata =
        new LuceneKeyAndMetadata(
            new LuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata);

    // FIXME: index.buildQuery might not exist or have different signature
    return memoryIndex.search(index.buildQuery(keyAndMetadata)) > 0.0f;
  }

  private Document getMetadataDoc(Object[] params, int metadataParamIndex) { // Changed
    if (params.length > metadataParamIndex) {
      if (params[metadataParamIndex] instanceof Map) {
        return new Document().fromMap((Map<String, ?>) params[metadataParamIndex]); // Changed
      } else if (params[metadataParamIndex] instanceof String) {
        return new Document().fromJSON((String) params[metadataParamIndex]);
      }
      return new Document().fromJSON(params[metadataParamIndex].toString());
    }
    // FIXME: LuceneQueryBuilder.EMPTY_METADATA
    return new Document(); // LuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  public String getSyntax() {
    return "search_fields( <fieldNamesList>, <query>, [ <metadata> ] )"; // Class is implicit
  }

  // searchFromTarget and related metadata method from template might not be directly applicable
  // as this function operates on specified fields of current record using MemoryIndex.
  // If it were to support indexed execution, it would need to find a covering persistent index.
  @Override
  public Iterable<Identifiable> searchFromTarget(
      FromClause target,
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      Expression... args) {

    // This function, as implemented in execute(), builds an in-memory index for the current record.
    // For it to be "indexable" in a broader query, it would need to find a persistent Lucene index
    // that covers the requested fields for the target class.
    ArcadeLuceneFullTextIndex index = searchForIndex(target, ctx, args); // FIXME

    // First arg (args[0]) is fieldNamesList, second (args[1]) is query
    if (args.length < 2) throw new IllegalArgumentException("search_fields requires at least fieldNames and query parameters.");

    @SuppressWarnings("unchecked")
    // List<String> fieldNames = (List<String>) args[0].execute((Result) null, ctx); // This is how searchForIndex gets it.
    // We need the query string here.
    Expression queryExpression = args[1];
    String query = (String) queryExpression.execute((Result) null, ctx);


    if (index != null && query != null) {
      Document meta = getMetadata(args, ctx, 2); // Metadata is third arg (index 2)
      Set<Identifiable> luceneResultSet; // Changed
      try (Stream<RID> rids = // Changed
          // FIXME: index.getInternal().getRids() needs to be replaced
          index
              .getAssociatedIndex()
              .getRids( // This method might not exist
                  new LuceneKeyAndMetadata( // FIXME
                      new LuceneCompositeKey(Arrays.asList(query)).setContext(ctx), meta))) { // FIXME
        luceneResultSet = rids.collect(Collectors.toSet());
      }
      return luceneResultSet;
    }
    // Original threw RuntimeException, returning empty set might be safer for unhandled cases.
    return Collections.emptySet();
  }

  private Document getMetadata(Expression[] args, CommandContext ctx, int metadataParamIndex) { // Changed
    if (args.length > metadataParamIndex) {
      return getMetadata(args[metadataParamIndex], ctx); // Calls method from ArcadeLuceneSearchFunctionTemplate
    }
    // FIXME: LuceneQueryBuilder.EMPTY_METADATA
    return new Document(); // LuceneQueryBuilder.EMPTY_METADATA;
  }


  @Override
  protected ArcadeLuceneFullTextIndex searchForIndex( // Changed types
      FromClause target, CommandContext ctx, Expression... args) { // FIXME
    // First argument to the function (args[0]) is the list of field names
    if (args == null || args.length == 0) {
        throw new IllegalArgumentException("Field names list parameter is missing.");
    }
    Object fieldNamesParam = args[0].execute((Result) null, ctx);
    if (!(fieldNamesParam instanceof List)) {
        throw new IllegalArgumentException("Field names parameter must be a list.");
    }
    @SuppressWarnings("unchecked")
    List<String> fieldNames = (List<String>) fieldNamesParam;

    FromItem item = target.getItem(); // Changed
    Identifier identifier = item.getIdentifier(); // Changed
    String className = identifier.getStringValue();

    return searchForIndex(className, ctx, fieldNames); // Calls private helper
  }

  private ArcadeLuceneFullTextIndex searchForIndex( // Changed types
      String className, CommandContext ctx, List<String> fieldNames) {
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
            .filter(idx -> intersect(idx.getDefinition().getFields(), fieldNames))
            .collect(Collectors.toList());

    if (indices.size() > 1) {
      // If multiple indexes match (e.g. one on [f1], another on [f2], and we search [f1,f2])
      // This logic might need refinement. For now, it implies any single index covering *at least one* field.
      // The original code would throw "too many indices matching given field name" only if multiple INDIVIDUAL indexes
      // were found that EACH satisfy the intersect condition.
      // A more robust approach might be to find the "best" covering index or combine results if that makes sense.
      // For now, sticking to "if any index covers any of the fields, and there's only one such index"
      // The original code finds an index if ANY of its fields are in fieldNames.
      // If multiple such indexes exist, it's an error.

      // Let's find the one with the most matching fields? Or just the first one?
      // The original code would throw if 'indices.size() > 1'.
       throw new IllegalArgumentException(
          "Too many Lucene indices on class '" + className + "' match the specified fields: " + String.join(",", fieldNames)
          + ". Specify a single target index using search_index().");
    }

    return indices.size() == 0 ? null : indices.get(0);
  }

  // intersection and intersect methods are helpers, can remain as they are (generic)
  public <T> List<T> intersection(List<T> list1, List<T> list2) {
    List<T> list = new ArrayList<T>();
    for (T t : list1) {
      if (list2.contains(t)) {
        list.add(t);
      }
    }
    return list;
  }

  public <T> boolean intersect(List<T> list1, List<T> list2) {
    for (T t : list1) {
      if (list2.contains(t)) {
        return true;
      }
    }
    return false;
  }
}
