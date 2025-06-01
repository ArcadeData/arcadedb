package com.arcadedb.lucene.analyzer;

import static com.arcadedb.lucene.engine.ArcadeLuceneIndexEngineAbstract.RID; // FIXME: This might need to be ArcadeDB specific constant if RID definition changes

import com.arcadedb.lucene.index.ArcadeLuceneIndexType; // FIXME: Ensure this is the correct refactored class for OLuceneIndexType
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.analysis.core.KeywordAnalyzer;

/**
 * Created by frank on 10/12/15.
 *
 * <p>Doesn't allow to wrap components or readers. Thread local resources can be
 delegated to the
 * delegate analyzer, but not allocated on this analyzer (limit memory consumption). Uses a per
 * field reuse strategy.
 */
public class ArcadeLucenePerFieldAnalyzerWrapper extends DelegatingAnalyzerWrapper {
  private final Analyzer defaultDelegateAnalyzer;
  private final Map<String, Analyzer> fieldAnalyzers;

  /**
   * Constructs with default analyzer.
   *
   * @param defaultAnalyzer Any fields not specifically defined to use a different analyzer will use
   *     the one provided here.
   */
  public ArcadeLucenePerFieldAnalyzerWrapper(final Analyzer defaultAnalyzer) {
    this(defaultAnalyzer, new HashMap<>());
  }

  /**
   * Constructs with default analyzer and a map of analyzers to use for specific fields.
   *
   * @param defaultAnalyzer Any fields not specifically defined to use a different analyzer will use
   *     the one provided here.
   * @param fieldAnalyzers a Map (String field name to the Analyzer) to be used for those fields
   */
  public ArcadeLucenePerFieldAnalyzerWrapper(
      final Analyzer defaultAnalyzer, final Map<String, Analyzer> fieldAnalyzers) {
    super(PER_FIELD_REUSE_STRATEGY);
    this.defaultDelegateAnalyzer = defaultAnalyzer;
    this.fieldAnalyzers = new HashMap<>();

    this.fieldAnalyzers.putAll(fieldAnalyzers);

    this.fieldAnalyzers.put(RID, new KeywordAnalyzer());
    this.fieldAnalyzers.put(ArcadeLuceneIndexType.RID_HASH, new KeywordAnalyzer());
    this.fieldAnalyzers.put("_CLASS", new KeywordAnalyzer());
    this.fieldAnalyzers.put("_CLUSTER", new KeywordAnalyzer());
    this.fieldAnalyzers.put("_JSON", new KeywordAnalyzer());
  }

  @Override
  protected Analyzer getWrappedAnalyzer(final String fieldName) {
    final Analyzer analyzer = fieldAnalyzers.get(fieldName);
    return (analyzer != null) ? analyzer : defaultDelegateAnalyzer;
  }

  @Override
  public String toString() {
    return "ArcadeLucenePerFieldAnalyzerWrapper(" // Updated class name in toString
        + fieldAnalyzers
        + ", default="
        + defaultDelegateAnalyzer
        + ")";
  }

  public ArcadeLucenePerFieldAnalyzerWrapper add(final String field, final Analyzer analyzer) {
    fieldAnalyzers.put(field, analyzer);
    return this;
  }

  public ArcadeLucenePerFieldAnalyzerWrapper add(final ArcadeLucenePerFieldAnalyzerWrapper wrapper) { // Changed parameter type
    fieldAnalyzers.putAll(wrapper.getAnalyzers());
    return this;
  }

  public ArcadeLucenePerFieldAnalyzerWrapper remove(final String field) {
    fieldAnalyzers.remove(field);
    return this;
  }

  protected Map<String, Analyzer> getAnalyzers() {
    return fieldAnalyzers;
  }
}
