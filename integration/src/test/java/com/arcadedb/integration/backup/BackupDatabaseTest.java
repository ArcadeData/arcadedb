package com.arcadedb.integration.backup;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class BackupDatabaseTest extends TestHelper {

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(Path.of("target/backups").toFile());
  }

  @Test
  public void testBackup() {
    ResultSet resultSet = database.command("sql", "backup database file://test-backup.zip ");

    Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<String>getProperty("backupFile")).isEqualTo("file://test-backup.zip");
  }

  @Test
  public void testBackupEncryption() {
    ResultSet resultSet = database.command("sql", "backup database file://test-backup.zip with encryptionKey = 'SuperSecretKey'");

    Result result = resultSet.next();
    assertThat(result.<String>getProperty("result")).isEqualTo("OK");
    assertThat(result.<String>getProperty("backupFile")).isEqualTo("file://test-backup.zip");
  }
}
