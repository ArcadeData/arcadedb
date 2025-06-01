package com.arcadedb.lucene.functions;

import com.arcadedb.lucene.collections.OLuceneResultSet;
import com.arcadedb.lucene.index.OLuceneFullTextIndex;
import com.arcadedb.database.OCommandContext;
import com.arcadedb.database.OIdentifiable;
import com.arcadedb.database.record.impl.ODocument;
import com.arcadedb.database.sql.executor.OResult;
import com.arcadedb.database.sql.functions.OIndexableSQLFunction;
import com.arcadedb.database.sql.functions.OSQLFunctionAbstract;
import com.arcadedb.database.sql.parser.OBinaryCompareOperator;
import com.arcadedb.database.sql.parser.OExpression;
import com.arcadedb.database.sql.parser.OFromClause;
import java.util.Map;

/** Created by frank on 25/05/2017. */
public abstract class OLuceneSearchFunctionTemplate extends OSQLFunctionAbstract
    implements OIndexableSQLFunction {

  public OLuceneSearchFunctionTemplate(String iName, int iMinParams, int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  @Override
  public boolean canExecuteInline(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    return allowsIndexedExecution(target, operator, rightValue, ctx, args);
  }

  @Override
  public boolean allowsIndexedExecution(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    OLuceneFullTextIndex index = searchForIndex(target, ctx, args);
    return index != null;
  }

  @Override
  public boolean shouldExecuteAfterSearch(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    return false;
  }

  @Override
  public long estimate(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {

    Iterable<OIdentifiable> a = searchFromTarget(target, operator, rightValue, ctx, args);
    if (a instanceof OLuceneResultSet) {
      return ((OLuceneResultSet) a).size();
    }
    long count = 0;
    for (Object o : a) {
      count++;
    }

    return count;
  }

  protected ODocument getMetadata(OExpression metadata, OCommandContext ctx) {
    final Object md = metadata.execute((OResult) null, ctx);
    if (md instanceof ODocument) {
      return (ODocument) md;
    } else if (md instanceof Map) {
      return new ODocument().fromMap((Map<String, ?>) md);
    } else if (md instanceof String) {
      return new ODocument().fromJSON((String) md);
    } else {
      return new ODocument().fromJSON(metadata.toString());
    }
  }

  protected abstract OLuceneFullTextIndex searchForIndex(
      OFromClause target, OCommandContext ctx, OExpression... args);
}
