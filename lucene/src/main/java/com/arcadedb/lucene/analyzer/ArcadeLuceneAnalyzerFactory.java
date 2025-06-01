package com.arcadedb.lucene.analyzer;

import com.arcadedb.document.Document;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.IndexException;
import com.arcadedb.index.IndexDefinition;
import com.arcadedb.schema.Type;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/** Created by frank on 30/10/2015. */
public class ArcadeLuceneAnalyzerFactory {
  private static final Logger logger = Logger.getLogger(ArcadeLuceneAnalyzerFactory.class.getName());

  public Analyzer createAnalyzer(
      final IndexDefinition index, final AnalyzerKind kind, final Document metadata) {
    if (index == null) {
      throw new IllegalArgumentException("Index must not be null");
    }
    if (kind == null) {
      throw new IllegalArgumentException("Analyzer kind must not be null");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("Metadata must not be null");
    }
    final String defaultAnalyzerFQN = metadata.getString("default");
    final String prefix = index.getTypeName() + ".";

    final OLucenePerFieldAnalyzerWrapper analyzer = // FIXME: Needs to be ArcadeLucenePerFieldAnalyzerWrapper
        geLucenePerFieldPresetAnalyzerWrapperForAllFields(defaultAnalyzerFQN);
    setDefaultAnalyzerForRequestedKind(index, kind, metadata, prefix, analyzer);
    setSpecializedAnalyzersForEachField(index, kind, metadata, prefix, analyzer);
    return analyzer;
  }

  private OLucenePerFieldAnalyzerWrapper geLucenePerFieldPresetAnalyzerWrapperForAllFields( // FIXME: Needs to be ArcadeLucenePerFieldAnalyzerWrapper
      final String defaultAnalyzerFQN) {
    if (defaultAnalyzerFQN == null) {
      return new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer()); // FIXME: Needs to be ArcadeLucenePerFieldAnalyzerWrapper
    } else {
      return new OLucenePerFieldAnalyzerWrapper(buildAnalyzer(defaultAnalyzerFQN)); // FIXME: Needs to be ArcadeLucenePerFieldAnalyzerWrapper
    }
  }

  private void setDefaultAnalyzerForRequestedKind(
      final IndexDefinition index,
      final AnalyzerKind kind,
      final Document metadata,
      final String prefix,
      final OLucenePerFieldAnalyzerWrapper analyzer) { // FIXME: Needs to be ArcadeLucenePerFieldAnalyzerWrapper
    final String specializedAnalyzerFQN = metadata.getString(kind.toString());
    if (specializedAnalyzerFQN != null) {
      for (final String field : index.getFields()) {
        analyzer.add(field, buildAnalyzer(specializedAnalyzerFQN));
        analyzer.add(prefix + field, buildAnalyzer(specializedAnalyzerFQN));
      }
    }
  }

  private void setSpecializedAnalyzersForEachField(
      final IndexDefinition index,
      final AnalyzerKind kind,
      final Document metadata,
      final String prefix,
      final OLucenePerFieldAnalyzerWrapper analyzer) { // FIXME: Needs to be ArcadeLucenePerFieldAnalyzerWrapper
    for (final String field : index.getFields()) {
      final String analyzerName = field + "_" + kind.toString();
      final String analyzerStopwords = analyzerName + "_stopwords";

      if (metadata.containsField(analyzerName) && metadata.containsField(analyzerStopwords)) {
        final Collection<String> stopWords = metadata.get(analyzerStopwords, Collection.class);
        analyzer.add(field, buildAnalyzer(metadata.getString(analyzerName), stopWords));
        analyzer.add(prefix + field, buildAnalyzer(metadata.getString(analyzerName), stopWords));
      } else if (metadata.containsField(analyzerName)) {
        analyzer.add(field, buildAnalyzer(metadata.getString(analyzerName)));
        analyzer.add(prefix + field, buildAnalyzer(metadata.getString(analyzerName)));
      }
    }
  }

  private Analyzer buildAnalyzer(final String analyzerFQN) {
    try {
      final Class<?> classAnalyzer = Class.forName(analyzerFQN);
      final Constructor<?> constructor = classAnalyzer.getDeclaredConstructor();
      return (Analyzer) constructor.newInstance();
    } catch (final ClassNotFoundException e) {
      throw new IndexException("Analyzer: " + analyzerFQN + " not found", e);
    } catch (final NoSuchMethodException e) {
      Class<?> classAnalyzer;
      try {
        classAnalyzer = Class.forName(analyzerFQN);
        //noinspection deprecation
        return (Analyzer) classAnalyzer.newInstance();
      } catch (Exception e1) {
        logger.log(Level.SEVERE, "Exception is suppressed, original exception is ", e);
        //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
        throw new IndexException("Couldn't instantiate analyzer:  public constructor  not found", e1);
      }
    } catch (Exception e) {
      logger.log(
          Level.SEVERE, "Error on getting analyzer for Lucene index (continuing with StandardAnalyzer)", e);
      return new StandardAnalyzer();
    }
  }

  private Analyzer buildAnalyzer(final String analyzerFQN, final Collection<String> stopwords) {
    try {
      final Class<?> classAnalyzer = Class.forName(analyzerFQN);
      final Constructor<?> constructor = classAnalyzer.getDeclaredConstructor(CharArraySet.class);
      return (Analyzer) constructor.newInstance(new CharArraySet(stopwords, true));
    } catch (final ClassNotFoundException e) {
      throw new IndexException("Analyzer: " + analyzerFQN + " not found", e);
    } catch (final NoSuchMethodException e) {
      throw new IndexException("Couldn't instantiate analyzer: public constructor not found", e);
    } catch (final Exception e) {
      logger.log(
          Level.SEVERE, "Error on getting analyzer for Lucene index (continuing with StandardAnalyzer)", e);
      return new StandardAnalyzer();
    }
  }

  public enum AnalyzerKind {
    INDEX,
    QUERY;

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ENGLISH);
    }
  }
}
