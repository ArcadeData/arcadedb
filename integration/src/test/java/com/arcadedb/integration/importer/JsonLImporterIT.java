package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

public class JsonLImporterIT {

  private final static String DATABASE_PATH = "target/databases/arcadedb-jsonl-importer";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(DATABASE_PATH));

  }

  @Test
  void importDatabase() {
    final File databaseDirectory = new File(DATABASE_PATH);

    final URL inputFile = getClass().getClassLoader().getResource("arcadedb-export.jsonl.tgz");

    Importer importer = new Importer(
        ("-url " + inputFile.getFile() + " -database " + DATABASE_PATH + " -forceDatabaseCreate true").split(" "));
    importer.load();

    assertThat(databaseDirectory.exists()).isTrue();
    try (final Database db = new DatabaseFactory(DATABASE_PATH).open()) {

      Schema schema = db.getSchema();

      //scheck schema
      assertThat(schema.getDateFormat()).isEqualTo("yyyy-MM-dd");
      assertThat(schema.getDateTimeFormat()).isEqualTo("yyyy-MM-dd HH:mm:ss");
      assertThat(schema.getZoneId()).isEqualTo(ZoneId.of("Europe/Zurich"));

      //check types
      assertThat(schema.getTypes()).hasSize(2);
      assertThat(schema.getType("Person")).isNotNull().satisfies(type -> {
        assertThat(type.getProperty("id").getType()).isEqualTo(Type.INTEGER);
        assertThat(type.getIndexesByProperties("id").get(0)).satisfies(index -> {
          assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
          assertThat(index.getNullStrategy()).isEqualTo(NULL_STRATEGY.SKIP);
        });
      });
      assertThat(schema.getType("Friend")).isNotNull().satisfies(type -> {
        assertThat(type.getProperty("id").getType()).isEqualTo(Type.INTEGER);
        assertThat(type.getIndexesByProperties("id").get(0)).satisfies(index -> {
          assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
          assertThat(index.getNullStrategy()).isEqualTo(NULL_STRATEGY.SKIP);
        });
      });

      //check vertices
      assertThat(db.countType("Person", true)).isEqualTo(500);


      //check edges
      assertThat(db.countType("Friend", true)).isEqualTo(10000);

    }

  }

}
