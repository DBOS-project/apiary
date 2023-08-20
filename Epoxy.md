# Epoxy

This repository contains an implementation of [Epoxy (VLDB 23)](http://petereliaskraft.net/res/p2732-kraft.pdf), a protocol for providing transactions across heterogeneous data stores.  We use Epoxy to allow Apiary functions to transactionally call functions accessing data stores.

For example, the first function performs a write in Postgres, then transactionally calls another function performing a write in Mongo, executing both writes in the same transaction:

```java
public class PostgresAddHotel extends PostgresFunction {

    private static final String insert = "INSERT INTO HotelsTable(HotelID, HotelName, AvailableRooms) VALUES (?, ?, ?);";

    public static int PostgresAddHotel(PostgresContext ctxt, int hotelID, String hotelName, int numRooms, int longitude, int latitude)throws Exception {
        ctxt.executeUpdate(insert, hotelID, hotelName, numRooms);
        ctxt.apiaryCallFunction("MongoAddHotel", hotelID, longitude, latitude);
        return hotelID;
    }
}

public class MongoAddHotel extends MongoFunction {

    public int runFunction(MongoContext context, int hotelID, int longitude, int latitude) throws PSQLException {
        Document hotel = new Document("hotelID", hotelID).append("point", new Point(new Position(longitude, latitude)));
        context.insertOne("hotels", hotel, Integer.toString(hotelID));
        return hotelID;
    }
}
```

Source code for the transaction coordinator (using Postgres as the primary database) is [here](src/main/java/org/dbos/apiary/postgres/PostgresConnection.java) and [here](src/main/java/org/dbos/apiary/postgres/PostgresContext.java).  Source code for the shims is [here](src/main/java/org/dbos/apiary/gcs) for GCS, [here](src/main/java/org/dbos/apiary/mongo) for MongoDB, [here](src/main/java/org/dbos/apiary/elasticsearch) for Elasticsearch, and [here](src/main/java/org/dbos/apiary/mysql) for MySQL.

Experiments for the paper were run in [this branch](https://github.com/DBOS-project/apiary/tree/epoxy-revision).  Code for the benchmarks can be found [here](https://github.com/DBOS-project/apiary/tree/epoxy-revision/src/main/java/org/dbos/apiary/benchmarks).  The procedures called by the benchmarks are [here](https://github.com/DBOS-project/apiary/tree/epoxy-revision/src/main/java/org/dbos/apiary/procedures).