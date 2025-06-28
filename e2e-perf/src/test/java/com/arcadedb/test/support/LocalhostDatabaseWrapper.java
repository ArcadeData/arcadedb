package com.arcadedb.test.support;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.remote.RemoteSchema;
import com.arcadedb.remote.RemoteServer;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static com.arcadedb.test.support.ContainersTestTemplate.DATABASE;
import static com.arcadedb.test.support.ContainersTestTemplate.PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;

public class LocalhostDatabaseWrapper {
  private static final Logger            logger = LoggerFactory.getLogger(LocalhostDatabaseWrapper.class);
  private final        RemoteDatabase    db;
  private final        Supplier<Integer> idSupplier;
  private final        Timer             photosTimer;
  private final        Timer             usersTimer;
  private final        Timer             friendshipTimer;

  public LocalhostDatabaseWrapper(Supplier<Integer> idSupplier) {
    this.db = connectToDatabase();
    this.idSupplier = idSupplier;
    usersTimer = Metrics.timer("arcadedb.test.inserted.users");
    photosTimer = Metrics.timer("arcadedb.test.inserted.photos");
    friendshipTimer = Metrics.timer("arcadedb.test.inserted.friendship");
  }

  private RemoteDatabase connectToDatabase() {
    RemoteDatabase database = new RemoteDatabase("localhost",
        2480,
        DATABASE,
        "root",
        "pippopluto");
    database.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    database.setTimeout(30000);
    return database;
  }

  public void close() {
    db.close();
  }

  public void createDatabase() {
    RemoteServer server = new RemoteServer("localhost",
        2480,
        "root",
        "pippopluto");
    server.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);

    if (server.exists(DATABASE)) {
      logger.info("Dropping existing database {}", DATABASE);
      server.drop(DATABASE);
    }
    server.create(DATABASE);
  }

  /**
   * This method creates the schema for the test.
   * It creates vertex types User and Photo, edge types HasUploaded, FriendOf, and Likes.
   * It also creates properties for User and Photo vertex types.
   */
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

            CREATE EDGE TYPE FriendOf;

            CREATE EDGE TYPE Likes;
            """);
  }

  /**
   * This method checks if the schema is created correctly.
   * It checks if the types User, Photo, HasUploaded, FriendOf, and Likes exist.
   */
  public void checkSchema() {
    RemoteSchema schema = db.getSchema();
    assertThat(schema.existsType("Photo")).isTrue();
    assertThat(schema.existsType("User")).isTrue();
    assertThat(schema.existsType("HasUploaded")).isTrue();
    assertThat(schema.existsType("FriendOf")).isTrue();
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
              {
                db.acquireLock()
                    .type("User")
                    .lock();
                db.command("sql", "CREATE VERTEX User SET id = ?", userId);
              }
              , true, 10);
        });

        addPhotosOfUser(userId, numberOfPhotos);

      } catch (Exception e) {
        Metrics.counter("arcadedb.test.inserted.users.error").increment();
        logger.error("Error creating user {}: {}", userId, e.getMessage());
        e.printStackTrace();
//        System.exit(1);
      }

    }
  }

  /**
   * This method adds photos for a specific user.
   * It creates a photo in a transaction with the user.
   *
   * @param userId         the id of the user to add photos for
   * @param numberOfPhotos the number of photos to add for the user
   */
  private void addPhotosOfUser(int userId, int numberOfPhotos) {
    for (int photoIndex = 1; photoIndex <= numberOfPhotos; photoIndex++) {
      int photoId = idSupplier.get();
      String photoName = String.format("download-%s.jpg", photoId);
      String sqlScript = """
          BEGIN;
          LOCK TYPE User, Photo, HasUploaded;
          LET photo = CREATE VERTEX Photo SET id = ?, name = ?;
          LET user = SELECT FROM User WHERE id = ?;
          LET userEdge = CREATE EDGE HasUploaded FROM $user TO $photo;
          COMMIT RETRY 30;
          RETURN $photo;""";
      try {
        photosTimer.record(() -> {
              db.command("sqlscript", sqlScript, photoId, photoName, userId);
            }
        );

      } catch (Exception e) {
        Metrics.counter("arcadedb.test.inserted.photos.error").increment();
        logger.error("Error creating photo {}: {}", photoId, e.getMessage());
      }
    }
  }

  /**
   * This method creates friendships between users.
   * It creates a number of friendships in iterations.
   * Each iteration creates a number of friendships between users.
   *
   * @param numOfFriendships the number of iterations to create friendships
   */
  public void createFriendships(int numOfFriendships) {
    int count = 0;
    UserIdSupplier userIdSupplier = new UserIdSupplier(db);
    while (count < numOfFriendships) {
      Integer userId1 = userIdSupplier.get();
      Integer userId2 = userIdSupplier.get();
      if (userId1 == null || userId2 == null || userId1.equals(userId2)) {
        continue; // Skip if no more users or same user
      }
      addFriendship(userId1, userId2);
      count++;
    }
    logger.info("Created {} friendships", count);
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
            db.transaction(() -> {
                  db.acquireLock()
                      .type("FriendOf")
                      .type("User")
                      .lock();
                  ResultSet resultSet = db.command("sql",
                      """
                          CREATE EDGE FriendOf
                          FROM (SELECT FROM User WHERE id = ?) TO (SELECT FROM User WHERE id = ?)
                          """, userId1, userId2);
                }
                , true, 30);

          }
      );

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

  public void assertThatFriendshipCountIs(int expectedCount) {
    assertThat(countFriendships()).isEqualTo(expectedCount);
  }

  public void assertThatPhotoCountIs(int expectedCount) {
    assertThat(countPhotos()).isEqualTo(expectedCount);
  }

  public long countUsers() {
    return db.countType("User", false);
  }

  public long countPhotos() {
    return db.countType("Photo", false);
  }

  public long countFriendships() {
    return db.countType("FriendOf", false);
  }

  public ResultSet command(String command, Object... args) {
    logger.info("Execute command: {}", command);
    return db.command("sql", command, args);
  }

  private static class UserIdSupplier implements Supplier<Integer> {

    private final RemoteDatabase db;

    private Iterator<Integer> idsIt;
    private int               skip;
    private int               numOfUsers;

    public UserIdSupplier(RemoteDatabase db) {
      this.db = db;
      skip = 0;
      numOfUsers = 100;
    }

    @Override
    public Integer get() {
      if (idsIt == null || !idsIt.hasNext()) {
        fetchNextBatch();
      }
      if (idsIt.hasNext()) {
        return idsIt.next();
      } else {
        return null; // No more users available
      }
    }

    private void fetchNextBatch() {
      ResultSet resultSet = db.query("sql", "SELECT id  FROM User ORDER BY id SKIP ? LIMIT ?", skip, numOfUsers);
      List<Integer> ids = resultSet.stream()
          .map(r -> r.<Integer>getProperty("id"))
          .toList();
      idsIt = ids.iterator();
      if (!ids.isEmpty()) {
        skip += ids.size(); // Update skip for the next batch
      }

    }
  }
}
