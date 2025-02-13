const { Client } = require("pg");
const { GenericContainer, Wait } = require("testcontainers");

describe("E2E tests using pg client", () => {
  jest.setTimeout(60000);

  let arcadedbContainer;
  let postgresClient;

  beforeAll(async () => {
    arcadedbContainer = await new GenericContainer("arcadedata/arcadedb:latest")
      .withExposedPorts(2480, 6379, 5432, 8182)
      .withEnvironment({
        JAVA_OPTS:
          "-Darcadedb.server.rootPassword=playwithdata -Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} -Darcadedb.server.plugins=Postgres:com.arcadedb.postgres.PostgresProtocolPlugin,GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin",
      })
      .withStartupTimeout(60000)
      .withWaitStrategy(Wait.forHttp("/api/v1/ready", 2480).forStatusCodeMatching((statusCode) => statusCode === 204))
      .start();
    const connectionString = `postgres://root:playwithdata@${arcadedbContainer.getHost()}:${arcadedbContainer.getMappedPort(5432)}/beer`;

    postgresClient = new Client(connectionString);
    await postgresClient.connect();
  });

  afterAll(async () => {
    await postgresClient.end();
    await arcadedbContainer.stop();
  });

  it("should run simple query", async () => {
    const query = "select from Beer limit 10";
    console.log("Executing query:", query);
    const res = await postgresClient.query(query);
    console.log("Query executed successfully.", res.rows.length, "rows returned.");
    expect(res.rows.length).toBe(10);
  });
});
