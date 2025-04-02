package com.arcadedb.resilience;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteServer;
import org.testcontainers.containers.GenericContainer;

import java.util.function.Supplier;

import static com.arcadedb.resilience.ResilienceTestTemplate.PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;

public class DatabaseWrapper {

  private final RemoteDatabase      db;
  private final Supplier<Integer>   idSupplier;
  private       GenericContainer<?> arcadeServer;

  public DatabaseWrapper(GenericContainer<?> arcadeContainer, Supplier<Integer> idSupplier) {
    this.db = createDatabase(arcadeContainer);
    this.idSupplier = idSupplier;
  }

  private RemoteDatabase createDatabase(GenericContainer<?> arcadeContainer) {
    this.arcadeServer = arcadeContainer;
    return new RemoteDatabase(arcadeContainer.getHost(),
        arcadeContainer.getMappedPort(2480),
        "ha-test",
        "root",
        PASSWORD);
  }

  public void createDatabase() {
    RemoteServer server = new RemoteServer(arcadeServer.getHost(),
        arcadeServer.getMappedPort(2480),
        "root",
        PASSWORD);
    if (!server.exists("ha-test"))
      server.create("ha-test");
  }

  public void checkSchema() {
    assertThat(db.getSchema().existsType("Photo")).isTrue();
    assertThat(db.getSchema().existsType("User")).isTrue();
    assertThat(db.getSchema().existsType("HasUploaded")).isTrue();
    assertThat(db.getSchema().existsType("IsFriendOf")).isTrue();
    assertThat(db.getSchema().existsType("Likes")).isTrue();
  }

  void createSchema() {
    //this is a test-double of HTTPGraphIT.testOneEdgePerTx test
    db.command("sqlscript",
        """
            CREATE VERTEX TYPE User;
            CREATE PROPERTY User.id STRING;
            CREATE INDEX ON  User (id) UNIQUE;
            CREATE VERTEX TYPE Photo;
            CREATE PROPERTY Photo.id STRING;
            CREATE INDEX ON Photo (id) UNIQUE;
            CREATE EDGE TYPE HasUploaded;
            CREATE EDGE TYPE IsFriendOf;
            CREATE EDGE TYPE Likes;""");
  }

  void addUserAndPhotos(int numberOfUsers, int numberOfPhotos) {
    for (int userIndex = 1; userIndex <= numberOfUsers; userIndex++) {
      String userId = String.format("u%04d", idSupplier.get());
      db.command("sql", String.format("CREATE VERTEX User SET id = '%s'", userId));

      for (int photoIndex = 1; photoIndex <= numberOfPhotos; photoIndex++) {
        String photoId = String.format("p%04d", idSupplier.get());
        String photoName = String.format("download-%s.jpg", photoId);
        String sqlScript = String.format("""
            BEGIN;
            LET photo = CREATE VERTEX Photo SET id = '%s', name = '%s';
            LET user = SELECT FROM User WHERE id = '%s';
            LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo SET kind = 'User_Photos';
            COMMIT RETRY 30;
            RETURN $photo;""", photoId, photoName, userId);
        db.command("sqlscript", sqlScript);
      }
    }
  }

  public void assertThatUserCountIs(int expectedCount) {

    assertThat(countUsers()).isEqualTo(expectedCount);
  }

  public int countUsers() {
    ResultSet resultSet = db.query("sql", "SELECT count() as count FROM User");
    return resultSet.next().<Integer>getProperty("count");
  }

  public void assertThatPhotoCountIs(int expectedCount) {
    assertThat(countPhotos()).isEqualTo(expectedCount);
  }

  public int countPhotos() {
    ResultSet resultSet = db.query("sql", "SELECT count() as count FROM Photo");
    return resultSet.next().<Integer>getProperty("count");
  }
}
