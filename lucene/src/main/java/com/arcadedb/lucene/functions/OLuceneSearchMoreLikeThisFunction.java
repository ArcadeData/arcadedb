package com.arcadedb.lucene.functions;

import com.arcadedb.common.exception.OException;
import com.arcadedb.common.io.OIOException;
import com.arcadedb.common.log.OLogManager;
import com.arcadedb.common.log.OLogger;
import com.arcadedb.lucene.collections.OLuceneCompositeKey;
import com.arcadedb.lucene.index.OLuceneFullTextIndex;
import com.arcadedb.lucene.query.OLuceneKeyAndMetadata;
import com.arcadedb.database.OCommandContext;
import com.arcadedb.database.ODatabaseSession;
import com.arcadedb.database.OIdentifiable;
import com.arcadedb.database.id.ORID;
import com.arcadedb.database.id.ORecordId;
import com.arcadedb.database.metadata.OMetadataInternal;
import com.arcadedb.database.record.OElement;
import com.arcadedb.database.record.ORecord;
import com.arcadedb.database.record.impl.ODocument;
import com.arcadedb.database.sql.executor.OResult;
import com.arcadedb.database.sql.functions.OIndexableSQLFunction;
import com.arcadedb.database.sql.functions.OSQLFunctionAbstract;
import com.arcadedb.database.sql.parser.OBinaryCompareOperator;
import com.arcadedb.database.sql.parser.OExpression;
import com.arcadedb.database.sql.parser.OFromClause;
import com.arcadedb.database.sql.parser.OFromItem;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/** Created by frank on 15/01/2017. */
public class OLuceneSearchMoreLikeThisFunction extends OSQLFunctionAbstract
    implements OIndexableSQLFunction {

  private static final OLogger logger =
      OLogManager.instance().logger(OLuceneSearchMoreLikeThisFunction.class);

  public static final String NAME = "search_more";

  public OLuceneSearchMoreLikeThisFunction() {
    super(OLuceneSearchMoreLikeThisFunction.NAME, 1, 2);
  }

  @Override
  public String getName() {
    return OLuceneSearchMoreLikeThisFunction.NAME;
  }

  @Override
  public Object execute(
      Object iThis,
      OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] params,
      OCommandContext ctx) {

    // TODO: slow implementation can be made faster
    if (!(iCurrentRecord instanceof ODocument)) {
      return false;
    }
    String className = ((ODocument) iCurrentRecord).getClassName();
    OLuceneFullTextIndex index = this.searchForIndex(ctx, className);

    if (index == null) return Collections.emptySet();

    IndexSearcher searcher = index.searcher();

    ODocument metadata = new ODocument((Map) params[1]);

    List<String> ridsAsString = parseRidsObj(ctx, params[0]);

    List<ORecord> others =
        ridsAsString.stream()
            .map(
                rid -> {
                  ORecordId recordId = new ORecordId();

                  recordId.fromString(rid);
                  return recordId;
                })
            .map(id -> id.<ORecord>getRecord())
            .collect(Collectors.toList());

    MoreLikeThis mlt = buildMoreLikeThis(index, searcher, metadata);

    Builder queryBuilder = new Builder();

    excludeOtherFromResults(ridsAsString, queryBuilder);

    ODatabaseSession contest = ctx.getDatabase();
    addLikeQueries(others, mlt, queryBuilder, contest);

    Query mltQuery = queryBuilder.build();

    Set<OIdentifiable> luceneResultSet;
    try (Stream<ORID> rids =
        index
            .getInternal()
            .getRids(
                new OLuceneKeyAndMetadata(
                    new OLuceneCompositeKey(Arrays.asList(mltQuery.toString())).setContext(ctx),
                    metadata))) {
      luceneResultSet = rids.collect(Collectors.toSet());
    }

    return luceneResultSet.contains(iCurrentRecord);
  }

  @Override
  public String getSyntax() {
    return "SEARCH_MORE( [rids], [ metdatada {} ] )";
  }

  @Override
  public Iterable<OIdentifiable> searchFromTarget(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {

    OLuceneFullTextIndex index = this.searchForIndex(target, ctx);

    if (index == null) return Collections.emptySet();

    IndexSearcher searcher = index.searcher();

    OExpression expression = args[0];

    ODocument metadata = parseMetadata(args);

    List<String> ridsAsString = parseRids(ctx, expression);

    List<ORecord> others =
        ridsAsString.stream()
            .map(
                rid -> {
                  ORecordId recordId = new ORecordId();

                  recordId.fromString(rid);
                  return recordId;
                })
            .map(id -> id.<ORecord>getRecord())
            .collect(Collectors.toList());

    MoreLikeThis mlt = buildMoreLikeThis(index, searcher, metadata);

    Builder queryBuilder = new Builder();

    excludeOtherFromResults(ridsAsString, queryBuilder);

    ODatabaseSession contest = ctx.getDatabase();
    addLikeQueries(others, mlt, queryBuilder, contest);

    Query mltQuery = queryBuilder.build();

    Set<OIdentifiable> luceneResultSet;
    try (Stream<ORID> rids =
        index
            .getInternal()
            .getRids(
                new OLuceneKeyAndMetadata(
                    new OLuceneCompositeKey(Arrays.asList(mltQuery.toString())).setContext(ctx),
                    metadata))) {
      luceneResultSet = rids.collect(Collectors.toSet());
    }

    return luceneResultSet;
  }

  private List<String> parseRids(OCommandContext ctx, OExpression expression) {

    Object expResult = expression.execute((OResult) null, ctx);
    return parseRidsObj(ctx, expResult);
  }

  private List<String> parseRidsObj(OCommandContext ctx, Object expResult) {
    // single rind
    if (expResult instanceof OIdentifiable) {
      return Collections.singletonList(((OIdentifiable) expResult).getIdentity().toString());
    }

    Iterator iter;
    if (expResult instanceof Iterable) {
      iter = ((Iterable) expResult).iterator();
    } else if (expResult instanceof Iterator) {
      iter = (Iterator) expResult;
    } else {
      return Collections.emptyList();
    }

    List<String> rids = new ArrayList<>();
    while (iter.hasNext()) {
      Object item = iter.next();
      if (item instanceof OResult) {
        if (((OResult) item).isElement()) {
          rids.add(((OResult) item).getIdentity().get().toString());
        } else {
          Set<String> properties = ((OResult) item).getPropertyNames();
          if (properties.size() == 1) {
            Object val = ((OResult) item).getProperty(properties.iterator().next());
            if (val instanceof OIdentifiable) {
              rids.add(((OIdentifiable) val).getIdentity().toString());
            }
          }
        }
      } else if (item instanceof OIdentifiable) {
        rids.add(((OIdentifiable) item).getIdentity().toString());
      }
    }
    return rids;
  }

  private ODocument parseMetadata(OExpression[] args) {
    ODocument metadata = new ODocument();
    if (args.length == 2) {
      metadata.fromJSON(args[1].toString());
    }
    return metadata;
  }

  private MoreLikeThis buildMoreLikeThis(
      OLuceneFullTextIndex index, IndexSearcher searcher, ODocument metadata) {

    try {
      MoreLikeThis mlt = new MoreLikeThis(searcher.getIndexReader());

      mlt.setAnalyzer(index.queryAnalyzer());

      mlt.setFieldNames(
          Optional.ofNullable(metadata.<List<String>>getProperty("fieldNames"))
              .orElse(index.getDefinition().getFields())
              .toArray(new String[] {}));

      mlt.setMaxQueryTerms(
          Optional.ofNullable(metadata.<Integer>getProperty("maxQueryTerms"))
              .orElse(MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));

      mlt.setMinTermFreq(
          Optional.ofNullable(metadata.<Integer>getProperty("minTermFreq"))
              .orElse(MoreLikeThis.DEFAULT_MIN_TERM_FREQ));

      mlt.setMaxDocFreq(
          Optional.ofNullable(metadata.<Integer>getProperty("maxDocFreq"))
              .orElse(MoreLikeThis.DEFAULT_MAX_DOC_FREQ));

      mlt.setMinDocFreq(
          Optional.ofNullable(metadata.<Integer>getProperty("minDocFreq"))
              .orElse(MoreLikeThis.DEFAULT_MAX_DOC_FREQ));

      mlt.setBoost(
          Optional.ofNullable(metadata.<Boolean>getProperty("boost"))
              .orElse(MoreLikeThis.DEFAULT_BOOST));

      mlt.setBoostFactor(
          Optional.ofNullable(metadata.<Float>getProperty("boostFactor")).orElse(1f));

      mlt.setMaxWordLen(
          Optional.ofNullable(metadata.<Integer>getProperty("maxWordLen"))
              .orElse(MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));

      mlt.setMinWordLen(
          Optional.ofNullable(metadata.<Integer>getProperty("minWordLen"))
              .orElse(MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));

      mlt.setMaxNumTokensParsed(
          Optional.ofNullable(metadata.<Integer>getProperty("maxNumTokensParsed"))
              .orElse(MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));

      mlt.setStopWords(
          (Set<?>)
              Optional.ofNullable(metadata.getProperty("stopWords"))
                  .orElse(MoreLikeThis.DEFAULT_STOP_WORDS));

      return mlt;
    } catch (IOException e) {
      throw OException.wrapException(new OIOException("Lucene IO Exception"), e);
    }
  }

  private void addLikeQueries(
      List<ORecord> others, MoreLikeThis mlt, Builder queryBuilder, ODatabaseSession contest) {
    others.stream()
        .map(or -> contest.<OElement>load(or))
        .forEach(
            element ->
                Arrays.stream(mlt.getFieldNames())
                    .forEach(
                        fieldName -> {
                          String property = element.getProperty(fieldName);
                          try {
                            Query fieldQuery = mlt.like(fieldName, new StringReader(property));
                            if (!fieldQuery.toString().isEmpty())
                              queryBuilder.add(fieldQuery, Occur.SHOULD);
                          } catch (IOException e) {
                            // FIXME handle me!
                            logger.error("Error during Lucene query generation", e);
                          }
                        }));
  }

  private void excludeOtherFromResults(List<String> ridsAsString, Builder queryBuilder) {
    ridsAsString.stream()
        .forEach(
            rid ->
                queryBuilder.add(
                    new TermQuery(new Term("RID", QueryParser.escape(rid))), Occur.MUST_NOT));
  }

  private OLuceneFullTextIndex searchForIndex(OFromClause target, OCommandContext ctx) {
    OFromItem item = target.getItem();

    String className = item.getIdentifier().getStringValue();

    return searchForIndex(ctx, className);
  }

  private OLuceneFullTextIndex searchForIndex(OCommandContext ctx, String className) {
    OMetadataInternal dbMetadata =
        (OMetadataInternal) ctx.getDatabase().activateOnCurrentThread().getMetadata();

    List<OLuceneFullTextIndex> indices =
        dbMetadata.getImmutableSchemaSnapshot().getClass(className).getIndexes().stream()
            .filter(idx -> idx instanceof OLuceneFullTextIndex)
            .map(idx -> (OLuceneFullTextIndex) idx)
            .collect(Collectors.toList());

    if (indices.size() > 1) {
      throw new IllegalArgumentException("too many full-text indices on given class: " + className);
    }

    return indices.size() == 0 ? null : indices.get(0);
  }

  @Override
  public long estimate(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    OLuceneFullTextIndex index = this.searchForIndex(target, ctx);

    if (index != null) return index.size();
    return 0;
  }

  @Override
  public boolean canExecuteInline(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {
    return false;
  }

  @Override
  public boolean allowsIndexedExecution(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {

    OLuceneFullTextIndex index = this.searchForIndex(target, ctx);

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
}
