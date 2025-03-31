package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.assertj.core.api.Assertions.assertThat;

public class TestInsertAndSelectWithThreadBucketSelectionStrategy {

  @BeforeEach
  void setUp() {
    GlobalConfiguration.DATE_TIME_FORMAT.setValue("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.resetAll();

  }

  @Test
  public void testInsertAndSelectWithThreadBucketSelectionStrategy() {
    try (DatabaseFactory databaseFactory = new DatabaseFactory("databases/test")) {
      if (databaseFactory.exists())
        databaseFactory.open().drop();

      try (Database db = databaseFactory.create()) {
        db.transaction(() -> {
          DocumentType dtProducts = db.getSchema().createDocumentType("Product");
          dtProducts.createProperty("name", Type.STRING);
          dtProducts.createProperty("type", Type.STRING);
          dtProducts.createProperty("start", Type.DATETIME_MICROS);
          dtProducts.createProperty("stop", Type.DATETIME_MICROS);
          dtProducts.createProperty("v", Type.STRING);
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
          dtProducts.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "type", "start", "stop");
          dtProducts.setBucketSelectionStrategy(new ThreadBucketSelectionStrategy());
        });
      }

      try (final Database database = databaseFactory.open()) {
        try {
          String name1, name2, type, version1, version2;

          final LocalDateTime queryStart, queryStop, validityStart, validityStop;
          name1 = "CS_OPER_MPL_ORBREF_20180707T231257_20190711T045744_0001.EEF";
          name2 = "CS_OPER_MPL_ORBREF_20180707T231257_20190711T045744_0002.EEF";
          type = "MPL_ORBREF";
          version1 = "0001";
          version2 = "0002";
          validityStart = LocalDateTime.parse("2018-07-07T23:12:57.000000",
              DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
          validityStop = LocalDateTime.parse("2019-07-11T04:57:44.000000",
              DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
          try {
            database.begin();
            String sqlString = "UPDATE Product SET name = ?, type = ?, start = ?, stop = ?, v = ? UPSERT WHERE name = ?";
            try (ResultSet resultSet = database.command("sql", sqlString, name1, type, validityStart, validityStop, version1,
                name1)) {
              assertThat((long) resultSet.nextIfAvailable().getProperty("count", 0)).isEqualTo(1);
            }
//            database.commit();
//            database.begin();
            try (ResultSet resultSet = database.command("sql", sqlString, name2, type, validityStart, validityStop, version2,
                name2)) {
              assertThat((long) resultSet.nextIfAvailable().getProperty("count", 0)).isEqualTo(1);
            }
            database.commit();
          } catch (Exception e) {
            e.printStackTrace();
            if (database.isTransactionActive()) {
              database.rollback();
            }
          }
          queryStart = LocalDateTime.parse("2019-05-05T00:12:38.236835",
              DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
          queryStop = LocalDateTime.parse("2019-05-05T00:10:37.288211",
              DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS"));
          String sqlString = "SELECT name, start, stop FROM Product WHERE type = ? AND start <= ? AND stop >= ? ORDER BY start DESC, stop DESC, v DESC LIMIT 1";
          int total = 0;
          try (ResultSet resultSet = database.query("sql", sqlString, type, queryStart, queryStop)) {
            assertThat(resultSet.hasNext()).isTrue();
            ++total;
          }
          assertThat(total).isEqualTo(1);
        } finally {
          database.drop();
        }
      }
    }
  }
}
