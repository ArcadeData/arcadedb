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
public class ArcadeLuceneSearchOnIndexFunction extends ArcadeLuceneSearchFunctionTemplate { // Changed base class

  // public static final String MEMORY_INDEX = "_memoryIndex"; // Already in ArcadeLuceneFunctionsUtils
  public static final String NAME = "search_index"; // OrientDB's name was luceneMatch, but class name implies search_index

  public ArcadeLuceneSearchOnIndexFunction() {
    super(NAME, 2, 3); // Using "search_index" as function name
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Object execute( // FIXME: Signature might change based on actual SQLFunctionAbstract in ArcadeDB
      Object iThis,
      Identifiable iCurrentRecord, // Changed
      Object iCurrentResult,
      Object[] params,
      CommandContext ctx) { // Changed
    if (iThis instanceof RID) { // Changed
      iThis = ((RID) iThis).getRecord();
    }
    if (iThis instanceof Identifiable) { // Changed
      iThis = new ResultInternal((Identifiable) iThis); // Changed
    }
    Result result = (Result) iThis; // Changed

    String indexName = (String) params[0];

    ArcadeLuceneFullTextIndex index = searchForIndex(ctx, indexName); // FIXME

    if (index == null) return false;

    String query = (String) params[1];

    MemoryIndex memoryIndex = ArcadeLuceneFunctionsUtils.getOrCreateMemoryIndex(ctx); // Use refactored util

    // FIXME: index.getDefinition() might be different.
    List<Object> key =
        index.getDefinition().getFields().stream()
            .map(s -> result.getProperty(s))
            .collect(Collectors.toList());

    // FIXME: index.buildDocument and index.indexAnalyzer might not exist or have different signatures
    // This part is highly dependent on ArcadeLuceneFullTextIndex refactoring.
    org.apache.lucene.document.Document luceneDoc = index.buildDocument(key, iCurrentRecord);
    if (luceneDoc != null) {
      for (IndexableField field : luceneDoc.getFields()) {
        memoryIndex.addField(field.name(), field.stringValue(), index.indexAnalyzer()); // Simplified, assuming stringValue is appropriate
      }
    }

    Document metadata = getMetadataDoc(params); // Changed ODocument
    // FIXME: LuceneCompositeKey and LuceneKeyAndMetadata need refactoring
    LuceneKeyAndMetadata keyAndMetadata =
        new LuceneKeyAndMetadata(
            new LuceneCompositeKey(Arrays.asList(query)).setContext(ctx), metadata);

    // FIXME: index.buildQuery might not exist or have different signature
    return memoryIndex.search(index.buildQuery(keyAndMetadata)) > 0.0f;
  }

  private Document getMetadataDoc(Object[] params) { // Changed ODocument
    if (params.length == 3) {
      if (params[2] instanceof Map) {
        return new Document().fromMap((Map<String, ?>) params[2]); // Changed
      } else if (params[2] instanceof String) {
        return new Document().fromJSON((String) params[2]);
      }
      // Fallback for other types, or throw error
      return new Document().fromJSON(params[2].toString());
    }
    // FIXME: LuceneQueryBuilder.EMPTY_METADATA needs to be accessible or defined differently
    return new Document(); // LuceneQueryBuilder.EMPTY_METADATA;
  }

  // getOrCreateMemoryIndex was moved to ArcadeLuceneFunctionsUtils

  @Override
  public String getSyntax() {
    return "search_index( <indexName>, <query>, [ <metadata> ] )"; // Updated syntax
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

    ArcadeLuceneFullTextIndex index = searchForIndex(target, ctx, args); // FIXME

    Expression expression = args[1];
    String query = (String) expression.execute((Result) null, ctx); // Changed
    if (index != null && query != null) {

      Document meta = getMetadata(args, ctx); // Changed

      List<Identifiable> luceneResultSet; // Changed
      try (Stream<RID> rids = // Changed
          // FIXME: index.getInternal().getRids() needs to be replaced with ArcadeDB equivalent
          // This whole block is highly dependent on ArcadeLuceneFullTextIndex and LuceneKeyAndMetadata refactoring
          index
              .getAssociatedIndex() // Assuming getAssociatedIndex() is the way, or index might be the LuceneIndexEngine itself
              .getRids( // This method might not exist on ArcadeDB's Index interface or ArcadeLuceneFullTextIndex
                  new LuceneKeyAndMetadata( // FIXME
                      new LuceneCompositeKey(Arrays.asList(query)).setContext(ctx), meta))) { // FIXME
        luceneResultSet = rids.collect(Collectors.toList());
      }

      return luceneResultSet;
    }
    return Collections.emptyList();
  }

  private Document getMetadata(Expression[] args, CommandContext ctx) { // Changed types
    if (args.length == 3) {
      return getMetadata(args[2], ctx); // Calls the method in ArcadeLuceneSearchFunctionTemplate
    }
    // FIXME: LuceneQueryBuilder.EMPTY_METADATA
    return new Document(); // LuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  protected ArcadeLuceneFullTextIndex searchForIndex( // Changed types
      FromClause target, CommandContext ctx, Expression... args) { // FIXME

    FromItem item = target.getItem(); // Changed
    Identifier identifier = item.getIdentifier(); // Changed
    // FIXME: This was calling a private searchForIndex, now it should call the one from ArcadeLuceneFunctionsUtils or similar.
    // For now, assuming the util class will be used by the concrete implementations.
    // This abstract method in template might need rethinking or this class needs its own way to get the index.
    // Let's assume for now it will use the utility.
    String indexNameFromArg = (String) args[0].execute((Result) null, ctx);
    // String className = identifier.getStringValue(); // This would be the class from FROM clause
    // We need the index name from the function argument.
    return ArcadeLuceneFunctionsUtils.getLuceneFullTextIndex(ctx, indexNameFromArg); // FIXME
  }

  // Removed private searchForIndex methods, assuming logic will consolidate or use ArcadeLuceneFunctionsUtils

  // getResult(OCommandContext) is part of OSQLFunction and likely not needed if SQLFunctionAbstract is different
  // @Override
  // public Object getResult(CommandContext ctx) { // Changed OCommandContext
  // return super.getResult(ctx);
  // }
}
