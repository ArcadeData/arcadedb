package com.arcadedb.lucene.functions;

import com.arcadedb.database.Identifiable; // Changed
import com.arcadedb.document.Document; // Changed
import com.arcadedb.lucene.collections.LuceneResultSet; // FIXME: Needs refactoring
import com.arcadedb.lucene.index.ArcadeLuceneFullTextIndex; // FIXME: Needs refactoring
import com.arcadedb.query.sql.executor.CommandContext; // Changed
import com.arcadedb.query.sql.executor.Result; // Changed
import com.arcadedb.query.sql.function.IndexableSQLFunction; // Assuming this exists
import com.arcadedb.query.sql.function.SQLFunctionAbstract; // Assuming this is the base class
import com.arcadedb.query.sql.parser.BinaryCompareOperator; // Changed
import com.arcadedb.query.sql.parser.Expression; // Changed
import com.arcadedb.query.sql.parser.FromClause; // Changed
import java.util.Map;

/** Created by frank on 25/05/2017. */
// Changed base class and interface
public abstract class ArcadeLuceneSearchFunctionTemplate extends SQLFunctionAbstract
    implements IndexableSQLFunction {

  public ArcadeLuceneSearchFunctionTemplate(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  // FIXME: Signature of these methods depends heavily on the actual ArcadeDB interfaces for IndexableSQLFunction
  @Override
  public boolean canExecuteInline(
      FromClause target,
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      Expression... args) { // Changed parameter types
    return allowsIndexedExecution(target, operator, rightValue, ctx, args);
  }

  @Override
  public boolean allowsIndexedExecution(
      FromClause target,
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      Expression... args) { // Changed parameter types
    ArcadeLuceneFullTextIndex index = searchForIndex(target, ctx, args); // FIXME
    return index != null;
  }

  @Override
  public boolean shouldExecuteAfterSearch(
      FromClause target,
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      Expression... args) { // Changed parameter types
    return false;
  }

  @Override
  public long estimate(
      FromClause target,
      BinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      Expression... args) { // Changed parameter types

    // FIXME: searchFromTarget is not defined in this template, assuming it's from OIndexableSQLFunction or a subclass
    // For now, commenting out as its direct equivalent/necessity in ArcadeDB is unclear without seeing concrete function implementation
    /*
    Iterable<Identifiable> a = searchFromTarget(target, operator, rightValue, ctx, args); // Changed OIdentifiable
    if (a instanceof LuceneResultSet) { // FIXME
      return ((LuceneResultSet) a).size(); // FIXME
    }
    long count = 0;
    for (Object o : a) {
      count++;
    }
    return count;
    */
    return 0; // Placeholder
  }

  protected Document getMetadata(Expression metadata, CommandContext ctx) { // Changed ODocument, OExpression, OCommandContext
    final Object md = metadata.execute((Result) null, ctx); // Changed OResult
    if (md instanceof Document) { // Changed ODocument
      return (Document) md;
    } else if (md instanceof Map) {
      return new Document().fromMap((Map<String, ?>) md); // Changed ODocument
    } else if (md instanceof String) {
      try {
        return new Document().fromJSON((String) md); // Changed ODocument
      } catch (Exception e) {
        // It might not be a JSON string, but the raw metadata string itself (e.g. analyzer class name)
        // This part needs careful review based on how metadata is actually passed and used.
        // For now, returning a document with a field containing the string.
        Document doc = new Document();
        doc.set("metadata", (String) md); // FIXME: Review this fallback for non-JSON metadata strings
        return doc;
      }
    } else if (metadata != null) {
       // Fallback if metadata is not null but not a recognized type, try its string representation as JSON
      try {
        return new Document().fromJSON(metadata.toString()); // Changed ODocument
      } catch (Exception e) {
        Document doc = new Document();
        doc.set("metadata", metadata.toString()); // FIXME: Review this fallback
        return doc;
      }
    }
    return new Document(); // Empty document if null or unparseable
  }

  // Changed OLuceneFullTextIndex, OFromClause, OCommandContext, OExpression
  protected abstract ArcadeLuceneFullTextIndex searchForIndex( // FIXME
      FromClause target, CommandContext ctx, Expression... args);
}
