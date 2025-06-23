package com.arcadedb.containers.support;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.RemoteSchema;
import com.arcadedb.remote.RemoteServer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.function.Supplier;

import static com.arcadedb.containers.support.ContainersTestTemplate.PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;

public class DatabaseWrapper {
  private static final Logger              logger = LoggerFactory.getLogger(DatabaseWrapper.class);
  private final        RemoteDatabase      db;
  private final        GenericContainer<?> arcadeServer;
  private final        Supplier<Integer>   idSupplier;
  private final        Timer               photosTimer;
  private final        Timer               usersTimer;
  private final        Timer               friendshipTimer;

  public DatabaseWrapper(GenericContainer<?> arcadeContainer, Supplier<Integer> idSupplier) {
    this.arcadeServer = arcadeContainer;
    this.db = connectToDatabase(arcadeContainer);
    this.idSupplier = idSupplier;
    usersTimer = Metrics.timer("arcadedb.test.inserted.users");
    photosTimer = Metrics.timer("arcadedb.test.inserted.photos");
    friendshipTimer = Metrics.timer("arcadedb.test.inserted.friendship");
  }

  private RemoteDatabase connectToDatabase(GenericContainer<?> arcadeContainer) {
    RemoteDatabase database = new RemoteDatabase(arcadeContainer.getHost(),
        arcadeContainer.getMappedPort(2480),
        "ha-test",
        "root",
        PASSWORD);
    database.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    return database;
  }

  public void close() {
    db.close();
  }

  public void createDatabase() {
    RemoteServer server = new RemoteServer(arcadeServer.getHost(),
        arcadeServer.getMappedPort(2480),
        "root",
        PASSWORD);
    server.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);

    if (server.exists("ha-test")) {
      logger.info("Dropping existing database ha-test");
      server.drop("ha-test");
    }
    if (!server.exists("ha-test"))
      server.create("ha-test");
  }

  public void createSchema() {
    //this is a test-double of HTTPGraphIT.testOneEdgePerTx test
    db.command("sqlscript",
        """
            CREATE VERTEX TYPE User;
            CREATE PROPERTY User.id INTEGER;
            CREATE INDEX ON User (id) UNIQUE;

            CREATE VERTEX TYPE Photo;
            CREATE PROPERTY Photo.id INTEGER;
            CREATE INDEX ON Photo (id) UNIQUE;

            CREATE EDGE TYPE HasUploaded;

            CREATE EDGE TYPE IsFriendOf;

            CREATE EDGE TYPE Likes;
            """);
  }

  public void checkSchema() {
    RemoteSchema schema = db.getSchema();
    assertThat(schema.existsType("Photo")).isTrue();
    assertThat(schema.existsType("User")).isTrue();
    assertThat(schema.existsType("HasUploaded")).isTrue();
    assertThat(schema.existsType("IsFriendOf")).isTrue();
    assertThat(schema.existsType("Likes")).isTrue();
  }

  /**
   * This method creates a number of users and photos for each user.
   * The photos are created in a transaction with the user.
   *
   * @param numberOfUsers  the number of users to create
   * @param numberOfPhotos the number of photos to create for each user
   */
  public void addUserAndPhotos(int numberOfUsers, int numberOfPhotos) {
    for (int userIndex = 1; userIndex <= numberOfUsers; userIndex++) {
      int userId = idSupplier.get();
      try {
        usersTimer.record(() -> {
          db.transaction(() ->
                  db.command("sql", "CREATE VERTEX User SET id = ?", userId)
              , true, 10);
        });

        addPhotosOfUser(userId, numberOfPhotos);

      } catch (Exception e) {
        Metrics.counter("arcadedb.test.inserted.users.error").increment();
        logger.error("Error creating user {}: {}", userId, e.getMessage());
      }

    }
  }

  private void addPhotosOfUser(int userId, int numberOfPhotos) {
    for (int photoIndex = 1; photoIndex <= numberOfPhotos; photoIndex++) {
      int photoId = idSupplier.get();
      String photoName = String.format("download-%s.jpg", photoId);
      String sqlScript = """
          BEGIN;
          LET photo = CREATE VERTEX Photo SET id = ?, name = ?;
          LET user = SELECT FROM User WHERE id = ?;
          LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo;
          COMMIT RETRY 30;
          RETURN $photo;""";
      try {
        photosTimer.record(() -> {
          db.transaction(() ->
                  db.command("sqlscript", sqlScript, photoId, photoName, userId)
              , true, 10);
        });

      } catch (Exception e) {
        Metrics.counter("arcadedb.test.inserted.photos.error").increment();
        logger.error("Error creating photo {}: {}", photoId, e.getMessage());
      }
    }
  }

  /**
   * This method creates a friendship between two users.
   * The friendship is created in a transaction with the users.
   *
   * @param userId1 the id of the first user
   * @param userId2 the id of the second user
   */
  public void addFriendship(int userId1, int userId2) {
    try {
      friendshipTimer.record(() -> {
            db.transaction(() ->
                {
                  db.acquireLock()
                      .type("IsFriendOf")
                      .type("User")
                      .lock();
                  db.command("sql",
                      """
                          CREATE EDGE IsFriendOf
                          FROM (SELECT FROM User WHERE id = ?) TO (SELECT FROM User WHERE id = ?)
                          """, userId1, userId2);

                }
                , true, 10);

          }
      );

    } catch (Exception e) {
      Metrics.counter("arcadedb.test.inserted.friendship.error").increment();
      logger.error("Error creating friendship between {} and {}: {}", userId1, userId2, e.getMessage());
    }
  }

  /**
   * This method creates a friendship between two users.
   * The friendship is created in a transaction with the users.
   *
   * @param userId1 the id of the first user
   * @param userId2 the id of the second user
   */
  public void addFriendshipScript(int userId1, int userId2) {
    try {
      friendshipTimer.record(() -> {
        db.transaction(() ->
            db.command("sqlscript",
                """
                    BEGIN;
                    CREATE EDGE IsFriendOf
                    FROM (SELECT FROM User WHERE id = ?) TO (SELECT FROM User WHERE id = ?);
                    COMMIT RETRY 30;
                    """, userId1, userId2), true);
      });

    } catch (Exception e) {
      Metrics.counter("arcadedb.test.inserted.friendship.error").increment();
      logger.error("Error creating friendship between {} and {}: {}", userId1, userId2, e.getMessage());
    }
  }

  public void assertThatUserCountIs(int expectedCount) {
    assertThat(countUsers()).isEqualTo(expectedCount);
  }

  public List<Integer> getUserIds(int numOfUsers, int skip) {
    ResultSet resultSet = db.query("sql", "SELECT id  FROM User ORDER BY id SKIP ? LIMIT ?", skip, numOfUsers);
    return resultSet.stream()
        .map(r -> r.<Integer>getProperty("id"))
        .toList();
  }

  public int countUsers() {
    ResultSet resultSet = db.query("sql", "SELECT count() as count FROM User");
    return resultSet.next().<Integer>getProperty("count");
  }

  public int countFriendships() {
    ResultSet resultSet = db.query("sql", "SELECT count() as count FROM IsFriendOf");
    return resultSet.next().<Integer>getProperty("count");
  }

  public void assertThatFriendshipCountIs(int expectedCount) {
    assertThat(countFriendships()).isEqualTo(expectedCount);
  }

  public void assertThatPhotoCountIs(int expectedCount) {
    assertThat(countPhotos()).isEqualTo(expectedCount);
  }

  public int countPhotos() {
    ResultSet resultSet = db.query("sql", "SELECT count() as count FROM Photo");
    return resultSet.next().<Integer>getProperty("count");
  }

  public ResultSet command(String command, Object... args) {
    logger.info("Execute command: {}", command);
    return db.command("sql", command, args);
  }
}
