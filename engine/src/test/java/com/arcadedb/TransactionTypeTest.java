package com.arcadedb;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

public class TransactionTypeTest extends TestHelper {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";

  @Test
  public void testPopulate() {
    // EMPTY METHOD
  }

  @Test
  public void testScan() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanType(TYPE_NAME, true, record -> {
      assertThat(record).isNotNull();

      final Set<String> prop = new HashSet<String>();
      prop.addAll(record.getPropertyNames());

      assertThat(record.getPropertyNames().size()).isEqualTo(3);
      assertThat(prop.contains("id")).isTrue();
      assertThat(prop.contains("name")).isTrue();
      assertThat(prop.contains("surname")).isTrue();

      total.incrementAndGet();
      return true;
    });

    assertThat(total.get()).isEqualTo(TOT);

    database.commit();
  }

  @Test
  public void testLookupAllRecordsByRID() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanType(TYPE_NAME, true, record -> {
      final Document record2 = (Document) database.lookupByRID(record.getIdentity(), false);
      assertThat(record2).isNotNull();
      assertThat(record2).isEqualTo(record);

      final Set<String> prop = new HashSet<String>();
      prop.addAll(record2.getPropertyNames());

      assertThat(record2.getPropertyNames().size()).isEqualTo(3);
      assertThat(prop.contains("id")).isTrue();
      assertThat(prop.contains("name")).isTrue();
      assertThat(prop.contains("surname")).isTrue();

      total.incrementAndGet();
      return true;
    });

    database.commit();

    assertThat(total.get()).isEqualTo(TOT);
  }

  @Test
  public void testLookupAllRecordsByKey() {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    for (int i = 0; i < TOT; i++) {
      final IndexCursor result = database.lookupByKey(TYPE_NAME, new String[] { "id" }, new Object[] { i });
      assertThat(Optional.ofNullable(result)).isNotNull();
      assertThat(result.hasNext()).isTrue();

      final Document record2 = (Document) result.next().getRecord();

      assertThat(record2.get("id")).isEqualTo(i);

      final Set<String> prop = new HashSet<String>();
      prop.addAll(record2.getPropertyNames());

      assertThat(record2.getPropertyNames().size()).isEqualTo(3);
      assertThat(prop.contains("id")).isTrue();
      assertThat(prop.contains("name")).isTrue();
      assertThat(prop.contains("surname")).isTrue();

      total.incrementAndGet();
    }

    database.commit();

    assertThat(total.get()).isEqualTo(TOT);
  }

  @Test
  public void testDeleteAllRecordsReuseSpace() throws IOException {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    database.scanType(TYPE_NAME, true, record -> {
      database.deleteRecord(record);
      total.incrementAndGet();
      return true;
    });

    database.commit();

    assertThat(total.get()).isEqualTo(TOT);

    database.begin();

    assertThat(database.countType(TYPE_NAME, true)).isEqualTo(0);

    // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
    final Index[] indexes = database.getSchema().getIndexes();
    for (int i = 0; i < TOT; ++i) {
      for (final Index index : indexes)
        assertThat(index.get(new Object[]{i}).hasNext()).as("Found item with key " + i).isFalse();
    }

    beginTest();

    database.transaction(() -> assertThat(database.countType(TYPE_NAME, true)).isEqualTo(TOT));
  }

  @Test
  public void testDeleteRecordsCheckScanAndIterators() throws IOException {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    final long originalCount = database.countType(TYPE_NAME, true);

    database.scanType(TYPE_NAME, true, record -> {
      database.deleteRecord(record);
      total.incrementAndGet();
      return false;
    });

    database.commit();

    assertThat(total.get()).isEqualTo(1);

    database.begin();

    assertThat(database.countType(TYPE_NAME, true)).isEqualTo(originalCount - 1);

    // COUNT WITH SCAN
    total.set(0);
    database.scanType(TYPE_NAME, true, record -> {
      total.incrementAndGet();
      return true;
    });
    assertThat(total.get()).isEqualTo(originalCount - 1);

    // COUNT WITH ITERATE TYPE
    total.set(0);
    for (final Iterator<Record> it = database.iterateType(TYPE_NAME, true); it.hasNext(); it.next())
      total.incrementAndGet();

    assertThat(total.get()).isEqualTo(originalCount - 1);
  }

  @Test
  public void testPlaceholderOnScanAndIterate() throws IOException {
    final AtomicInteger total = new AtomicInteger();

    database.begin();

    final long originalCount = database.countType(TYPE_NAME, true);

    database.scanType(TYPE_NAME, true, record -> {
      record.modify().set("additionalProperty", "Something just to create a placeholder").save();
      total.incrementAndGet();
      return false;
    });

    database.commit();

    assertThat(total.get()).isEqualTo(1);

    database.begin();

    assertThat(database.countType(TYPE_NAME, true)).isEqualTo(originalCount);

    // COUNT WITH SCAN
    total.set(0);
    database.scanType(TYPE_NAME, true, record -> {
      total.incrementAndGet();
      return true;
    });
    assertThat(total.get()).isEqualTo(originalCount);

    // COUNT WITH ITERATE TYPE
    total.set(0);
    for (final Iterator<Record> it = database.iterateType(TYPE_NAME, true); it.hasNext(); it.next())
      total.incrementAndGet();

    assertThat(total.get()).isEqualTo(originalCount);
  }

  @Test
  public void testDeleteFail() {
    reopenDatabaseInReadOnlyMode();

    assertThatExceptionOfType(DatabaseIsReadOnlyException.class).isThrownBy(() -> {

      database.begin();

      database.scanType(TYPE_NAME, true, record -> {
        database.deleteRecord(record);
        return true;
      });

      database.commit();
    });

    reopenDatabase();
  }

  @Test
  public void testNestedTx() {
    database.transaction(() -> {
      database.newDocument(TYPE_NAME).set("id", -1, "tx", 1).save();
      database.transaction(() -> database.newDocument(TYPE_NAME).set("id", -2, "tx", 2).save());
    });

    assertThat(CollectionUtils.countEntries(database.query("sql", "select from " + TYPE_NAME + " where tx = 0"))).isEqualTo(0);
    assertThat(CollectionUtils.countEntries(database.query("sql", "select from " + TYPE_NAME + " where tx = 1"))).isEqualTo(1);
    assertThat(CollectionUtils.countEntries(database.query("sql", "select from " + TYPE_NAME + " where tx = 2"))).isEqualTo(1);
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      if (!database.getSchema().existsType(TYPE_NAME)) {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
        type.createProperty("id", Integer.class);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, "id");
      }

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");

        v.save();
      }
    });
  }
}
