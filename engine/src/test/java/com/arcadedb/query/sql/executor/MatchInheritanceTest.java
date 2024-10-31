package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MatchInheritanceTest extends TestHelper {
  @Test
  public void testInheritance() {
    ResultSet result = null;

    result = database.command("SQL", "SELECT FROM Services");
    int selectFromServices = 0;
    while (result.hasNext()) {
      Result record = result.next();
      ++selectFromServices;
    }
    assertThat(selectFromServices).isEqualTo(4);

    result = database.command("SQL", "SELECT FROM Attractions");
    int selectFromAttractions = 0;
    while (result.hasNext()) {
      Result record = result.next();
      ++selectFromAttractions;
    }
    assertThat(selectFromAttractions).isEqualTo(4);

    result = database.command("SQL", "SELECT FROM Locations");
    int selectFromLocations = 0;
    while (result.hasNext()) {
      Result record = result.next();
      ++selectFromLocations;
    }
    assertThat(selectFromLocations).isEqualTo(8);

    result = database.query("SQL",
        "MATCH {type: Customers, as: customer, where: (OrderedId=1)}--{type: Monuments} RETURN $pathelements");

    assertThat(result.stream().count()).isEqualTo(2);

    result = database.query("SQL",
        "MATCH {type: Customers, as: customer, where: (OrderedId=1)}--{type: Services} RETURN $pathelements");

    assertThat(result.stream().count()).isEqualTo(8);

    result = database.query("SQL",
        "MATCH {type: Customers, as: customer, where: (OrderedId=1)}--{type: Attractions} RETURN $pathelements");

    assertThat(result.stream().count()).isEqualTo(8);

    result = database.query("SQL",
        "MATCH {type: Customers, as: customer, where: (OrderedId=1)}--{type: Locations} RETURN $pathelements");

    assertThat(result.stream().count()).isEqualTo(16);
  }

  @Override
  public void beginTest() {
/*
			-- Locations
			--    + Services
			--        + Hotels
			--        + Restaurants
			--    + Attractions
			--        + Monuments
			--        + Castles
			--        + Theatres
			--        + Archaeological Sites
*/
    String sqlScript = """
        BEGIN;
        CREATE VERTEX TYPE Locations;
        CREATE PROPERTY Locations.Id LONG;
        CREATE PROPERTY Locations.Type STRING;
        CREATE PROPERTY Locations.Name STRING;

        CREATE INDEX ON Locations (Type) NOTUNIQUE;
        CREATE INDEX ON Locations (Name) FULL_TEXT;

        CREATE VERTEX TYPE Services EXTENDS Locations;
        CREATE VERTEX TYPE Hotels EXTENDS Services;
        CREATE INDEX ON Hotels (Id) UNIQUE;

        CREATE VERTEX TYPE Restaurants EXTENDS Services;
        CREATE INDEX ON Restaurants(Id) UNIQUE;

        CREATE VERTEX TYPE Attractions EXTENDS Locations;
        CREATE VERTEX TYPE Monuments EXTENDS Attractions;
        CREATE INDEX ON Monuments (Id) UNIQUE;

        CREATE VERTEX TYPE Castles EXTENDS Attractions;
        CREATE INDEX ON Castles(Id) UNIQUE;

        CREATE VERTEX TYPE Theatres EXTENDS Attractions;
        CREATE INDEX ON Theatres(Id) UNIQUE;

        CREATE VERTEX TYPE ArchaeologicalSites EXTENDS Attractions;
        CREATE INDEX ON ArchaeologicalSites(Id) UNIQUE;

        CREATE VERTEX TYPE Customers;
        CREATE PROPERTY Customers.OrderedId LONG;

        CREATE VERTEX TYPE Orders;
        CREATE PROPERTY Orders.Id LONG;
        CREATE PROPERTY Orders.Amount LONG;
        CREATE PROPERTY Orders.OrderDate DATE;

        CREATE INDEX ON Customers(OrderedId) UNIQUE;

        CREATE INDEX ON Orders(Id) UNIQUE;

        CREATE EDGE TYPE HasUsedService;
        CREATE PROPERTY HasUsedService.out LINK OF Customers;

        CREATE EDGE TYPE HasStayed EXTENDS HasUsedService;
        CREATE PROPERTY HasStayed.in LINK OF Hotels;

        CREATE EDGE TYPE HasEaten EXTENDS HasUsedService;
        CREATE PROPERTY HasEaten.in LINK OF Restaurants;

        CREATE EDGE TYPE HasVisited;
        CREATE PROPERTY HasVisited.out LINK OF Customers;
        CREATE PROPERTY HasVisited.in LINK;
        CREATE INDEX ON HasVisited (`in`, `out`) UNIQUE;

        CREATE EDGE TYPE HasCustomer;
        CREATE PROPERTY HasCustomer.in LINK OF Customers;
        CREATE PROPERTY HasCustomer.out LINK OF Orders;

        INSERT INTO Customers SET OrderedId = 1, Phone = '+1400844724';
        INSERT INTO Orders SET Id = 1, Amount = 536, OrderDate = '2013-05-23';

        INSERT INTO Hotels SET Id = 730, Name = 'Toules', Type = 'alpine_hut';

        INSERT INTO Restaurants SET Id = 1834, Name = 'Uliassi', Type = 'restaurant';
        INSERT INTO Restaurants SET Id = 1099, Name = 'L\\'Angelo d\\'Oro', Type = 'restaurant';

        INSERT INTO Restaurants SET Id = 1738, Name = 'Johnny Paranza', Type = 'fast_food';

        INSERT INTO Castles SET Id = 127, Name = 'Haselburg', Type = 'castle';
        INSERT INTO ArchaeologicalSites SET Id = 47, Name = 'Villa Romana', Type = 'archaeological_site';
        INSERT INTO Monuments SET Id = 62, Name = 'Giuseppe Garibaldi', Type = 'monument';
        INSERT INTO Theatres SET Id = 65, Name = 'Teatro Civico', Type = 'theatre';

        CREATE EDGE HasStayed FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Hotels WHERE Id=730);

        CREATE EDGE HasEaten FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Restaurants WHERE Id=1834);
        CREATE EDGE HasEaten FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Restaurants WHERE Id=1099);
        CREATE EDGE HasEaten FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Restaurants WHERE Id=1738);

        CREATE EDGE HasCustomer FROM (SELECT FROM Orders WHERE Id=1) TO (SELECT FROM Customers WHERE OrderedId=1);

        CREATE EDGE HasVisited FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Castles WHERE Id=127);
        CREATE EDGE HasVisited FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM ArchaeologicalSites WHERE Id=47);
        CREATE EDGE HasVisited FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Monuments WHERE Id=62);
        CREATE EDGE HasVisited FROM (SELECT FROM Customers WHERE OrderedId=1) TO (SELECT FROM Theatres WHERE Id=65);

        COMMIT;
        """;
    database.command("SQLScript", sqlScript);
  }
}
