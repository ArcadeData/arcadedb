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
public class DatabaseAdminStatementsTest {
  private Database database;
  private StatementCache cache;

  @BeforeEach
  public void setup() {
    database = new DatabaseFactory("./target/test-databases/DatabaseAdminStatementsTest").create();
    cache = new StatementCache(database, 100);
  }

  @AfterEach
  public void teardown() {
    if (database != null) {
      database.drop();
      database = null;
    }
    TestHelper.checkActiveDatabases();
  }

  // ALIGN DATABASE tests
  @Test
  public void testAlignDatabaseBasic() {
    String sql = "ALIGN DATABASE";

    AlignDatabaseStatement stmt = (AlignDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }

  @Test
  public void testAlignDatabaseCaseInsensitive() {
    String sql = "align database";

    AlignDatabaseStatement stmt = (AlignDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }

  // IMPORT DATABASE tests
  @Test
  public void testImportDatabaseWithHttpUrl() {
    String sql = "IMPORT DATABASE 'http://www.example.com/data.json'";

    ImportDatabaseStatement stmt = (ImportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("http://www.example.com/data.json");
  }

  @Test
  public void testImportDatabaseWithFileUrl() {
    String sql = "IMPORT DATABASE 'file:///path/to/data.json'";

    ImportDatabaseStatement stmt = (ImportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("file:///path/to/data.json");
  }

  @Test
  public void testImportDatabaseWithDoubleQuotes() {
    String sql = "IMPORT DATABASE \"http://www.example.com/data.json\"";

    ImportDatabaseStatement stmt = (ImportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("http://www.example.com/data.json");
  }

  @Test
  public void testImportDatabaseCaseInsensitive() {
    String sql = "import database 'http://www.example.com/data.json'";

    ImportDatabaseStatement stmt = (ImportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }

  // EXPORT DATABASE tests
  @Test
  public void testExportDatabaseWithPath() {
    String sql = "EXPORT DATABASE 'backup.json'";

    ExportDatabaseStatement stmt = (ExportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("backup.json");
  }

  @Test
  public void testExportDatabaseWithFileUrl() {
    String sql = "EXPORT DATABASE 'file:///exports/backup.json'";

    ExportDatabaseStatement stmt = (ExportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("file:///exports/backup.json");
  }

  @Test
  public void testExportDatabaseWithDoubleQuotes() {
    String sql = "EXPORT DATABASE \"backup.jsonl.tgz\"";

    ExportDatabaseStatement stmt = (ExportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("backup.jsonl.tgz");
  }

  @Test
  public void testExportDatabaseCaseInsensitive() {
    String sql = "export database 'backup.json'";

    ExportDatabaseStatement stmt = (ExportDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }

  // BACKUP DATABASE tests
  @Test
  public void testBackupDatabaseWithPath() {
    String sql = "BACKUP DATABASE 'mybackup.zip'";

    BackupDatabaseStatement stmt = (BackupDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("mybackup.zip");
  }

  @Test
  public void testBackupDatabaseWithFileUrl() {
    String sql = "BACKUP DATABASE 'file:///backups/db-backup.zip'";

    BackupDatabaseStatement stmt = (BackupDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("file:///backups/db-backup.zip");
  }

  @Test
  public void testBackupDatabaseWithDoubleQuotes() {
    String sql = "BACKUP DATABASE \"mybackup.zip\"";

    BackupDatabaseStatement stmt = (BackupDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
    assertThat(stmt.url).isNotNull();
    assertThat(stmt.url.getUrlString()).isEqualTo("mybackup.zip");
  }

  @Test
  public void testBackupDatabaseCaseInsensitive() {
    String sql = "backup database 'mybackup.zip'";

    BackupDatabaseStatement stmt = (BackupDatabaseStatement) cache.get(sql);
    assertThat(stmt).isNotNull();
  }
}
