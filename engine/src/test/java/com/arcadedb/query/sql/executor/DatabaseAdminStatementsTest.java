package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.parser.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test database administration statements (ALIGN, IMPORT, EXPORT, BACKUP).
 * These tests verify that the parser correctly handles the admin statement syntax.
 */
class DatabaseAdminStatementsTest {
  private Database database;
  private StatementCache cache;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/test-databases/DatabaseAdminStatementsTest").create();
    cache = new StatementCache(database, 100);
  }

  @AfterEach
  void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    TestHelper.checkActiveDatabases();
  }

  // ALIGN DATABASE tests
  @Test
  void alignDatabaseBasic() {
    String sql = "ALIGN DATABASE";

    AlignDatabaseStatement stmt = (AlignDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }

  @Test
  void alignDatabaseCaseInsensitive() {
    String sql = "align database";

    AlignDatabaseStatement stmt = (AlignDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }

  // IMPORT DATABASE tests
  @Test
  void importDatabaseWithHttpUrl() {
    String sql = "IMPORT DATABASE 'http://www.example.com/data.json'";

    ImportDatabaseStatement stmt = (ImportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("http://www.example.com/data.json");
  }

  @Test
  void importDatabaseWithFileUrl() {
    String sql = "IMPORT DATABASE 'file:///path/to/data.json'";

    ImportDatabaseStatement stmt = (ImportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("file:///path/to/data.json");
  }

  @Test
  void importDatabaseWithDoubleQuotes() {
    String sql = "IMPORT DATABASE \"http://www.example.com/data.json\"";

    ImportDatabaseStatement stmt = (ImportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("http://www.example.com/data.json");
  }

  @Test
  void importDatabaseCaseInsensitive() {
    String sql = "import database 'http://www.example.com/data.json'";

    ImportDatabaseStatement stmt = (ImportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }

  // EXPORT DATABASE tests
  @Test
  void exportDatabaseWithPath() {
    String sql = "EXPORT DATABASE 'backup.json'";

    ExportDatabaseStatement stmt = (ExportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("backup.json");
  }

  @Test
  void exportDatabaseWithFileUrl() {
    String sql = "EXPORT DATABASE 'file:///exports/backup.json'";

    ExportDatabaseStatement stmt = (ExportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("file:///exports/backup.json");
  }

  @Test
  void exportDatabaseWithDoubleQuotes() {
    String sql = "EXPORT DATABASE \"backup.jsonl.tgz\"";

    ExportDatabaseStatement stmt = (ExportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("backup.jsonl.tgz");
  }

  @Test
  void exportDatabaseCaseInsensitive() {
    String sql = "export database 'backup.json'";

    ExportDatabaseStatement stmt = (ExportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }

  // BACKUP DATABASE tests
  @Test
  void backupDatabaseWithPath() {
    String sql = "BACKUP DATABASE 'mybackup.zip'";

    BackupDatabaseStatement stmt = (BackupDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("mybackup.zip");
  }

  @Test
  void backupDatabaseWithFileUrl() {
    String sql = "BACKUP DATABASE 'file:///backups/db-backup.zip'";

    BackupDatabaseStatement stmt = (BackupDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("file:///backups/db-backup.zip");
  }

  @Test
  void backupDatabaseWithDoubleQuotes() {
    String sql = "BACKUP DATABASE \"mybackup.zip\"";

    BackupDatabaseStatement stmt = (BackupDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("mybackup.zip");
  }

  @Test
  void backupDatabaseCaseInsensitive() {
    String sql = "backup database 'mybackup.zip'";

    BackupDatabaseStatement stmt = (BackupDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }

  @Test
  void backupDatabaseWithoutUrl() {
    // This should work - the URL is optional and defaults to server backup directory
    String sql = "BACKUP DATABASE";

    BackupDatabaseStatement stmt = (BackupDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNull();
  }
}
