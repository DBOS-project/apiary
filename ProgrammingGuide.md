# Programming Guide

This document provides a guide to programming in Apiary.  
Please also see our [tutorial](https://github.com/DBOS-project/apiary/blob/main/postgres-demo/README.md)
and documentation.
This document focuses on our Postgres DBMS backend, we also support VoltDB
and will add other databases in the future.

### Functions

An Apiary program is made up of _functions_.
Apiary provides two types of functions: transactional functions,
which can access or modify data and run as DBMS transactions,
and stateless functions, which do not access the database.
We expect transactional functions to be used for operations on data 
and stateless functions to be used for communicating with external services
and for compute-intensive operations like machine learning inference.

### Transactional Functions

To write a transactional function, simply subclass the `PostgresFunction`
class and implement your function in its `runFunction` method.
The first argument to a transactional function is always an `ApiaryTransactionalContext` object
automatically provided by Apiary and used to access its API.  Functions may
have any number of additional arguments but can only take in and return
integers, strings, arrays of integers, and arrays of strings.
Here is an example of a transactional function that registers users in a social network
taken from our  [tutorial](https://github.com/DBOS-project/apiary/blob/main/postgres-demo/README.md):

```java
public class NectarRegister extends PostgresFunction {

    private static final String checkExists = "SELECT * FROM WebsiteLogins WHERE Username=?";
    private static final String register = "INSERT INTO WebsiteLogins(Username, Password) VALUES (?, ?);";

    public static int runFunction(ApiaryTransactionalContext ctxt, String username, String password) throws SQLException {
        ResultSet exists = (ResultSet) ctxt.apiaryExecuteQuery(checkExists, username);
        if (exists.next()) {
            return 1;  // Failed registration, username already exists.
        }
        ctxt.apiaryExecuteUpdate(register, username, password);
        return 0;
    }
}
```

The `ApiaryTransactionalContext` object provides methods for updating and querying the database,
described in our documentation.  It also provides methods for calling other functions,
described in more detail below.

### Stateless Functions

To write a stateless function, simply subclass the `StatelessFunction` class
and implement your function in its `runFunction` method.
The first argument to a stateless function is always an `ApiaryStatelessContext` object
automatically provided by Apiary and used to access its API.  Functions may
have any number of additional arguments but can only take in and return
integers, strings, arrays of integers, and arrays of strings.
Here is a mockup of a stateless function that uses an external notification service
to send a notification to a user:

```java
public class Notify extends StatelessFunction {

    public static void runFunction(ApiaryStatelessContext context, String username, String message) {
        // Use an external service to notify the user
        // about some event, sending the input
        // message to them.
    }
}
```

### Composing Functions

Apiary functions can call one another
either synchronously or asynchronously.
To call a function synchronously, use the `callFunction` method of
the function context (`ApiaryTransactionalContext` or `ApiaryStatelessContext`).
Synchronous calls execute and return inside the caller transaction
(if the caller is transactional).
Here's an example of when you might want to call a function synchronously,
in a bank transfer where you need to validate the transfer, withdraw, and deposit,
all in the same transaction:

```java
public class BankTransfer extends PostgresFunction {
    public static int runFunction(ApiaryTransactionalContext context, 
                                   int senderAccountNumber, int receiverAccountNumber, int amount) {
        int validation = ctxt.apiaryCallFunction("org.example.bank.transfer.validate", senderAccountNumber, receiverAccountNumber, amount).getInt();
        if (validation == 0) { // Validation succeeded.
            ctxt.apiaryCallFunction("org.example.bank.transfer.withdraw", senderAccountNumber, receiverAccountNumber, amount);
            ctxt.apiaryCallFunction("org.example.bank.transfer.deposit", senderAccountNumber, receiverAccountNumber, amount);
            return 0;
        } else { // Validation did not succeed.
            return 1;
        }
    }
}
```

To call a function asynchronously, use the `queueFunction` method of the function context.
Asynchronous calls execute after their caller in a separate transaction.
They return a future which can be passed to another function or returned,
but cannot be dereferenced directly.
Here's an example of when you might want to call a function asynchronously,
where you want to notify a user that a message was sent to them but don't need to do it
in the same transaction that made the post:

```java
public class SendMessage extends PostgresFunction {

    private static final String addMessage = "INSERT INTO Messages(Sender, Receiver, MessageText) VALUES (?, ?, ?);";

    public static ApiaryFuture runFunction(ApiaryTransactionalContext ctxt, String sender, String receiver, String message) {
        ctxt.apiaryExecuteUpdate(addPost, sender, receiver, message);
        ApiaryFuture notificationSuccess = ctxt.apiaryQueueFunction("Notify", receiver, message);
        return notificationSuccess; // This will be dereferenced upon delievery, so the caller will receieve the actual success value.
    }
}
```

### Running Apiary: Workers and Clients

To run Apiary, you need to start up a worker that can communicate with the database
and run functions.  To do this, create a database connection
(e.g., `PostgresConnection`), use it to create tables,
and register your functions with it. Then, start a worker using that connection.
For example, here is the code to initialize a Postgres connection,
create the tables and functions used in the [tutorial](https://github.com/DBOS-project/apiary/tree/main/postgres-demo),
and start a worker:

```java
PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort, "postgres", "postgres", "dbos");
conn.createTable("WebsiteLogins", "Username VARCHAR(1000) PRIMARY KEY NOT NULL, Password VARCHAR(1000) NOT NULL");
conn.createTable("WebsitePosts", "Sender VARCHAR(1000) NOT NULL, Receiver VARCHAR(1000) NOT NULL, PostText VARCHAR(10000) NOT NULL");
conn.registerFunction("NectarRegister", NectarRegister::new);
conn.registerFunction("NectarLogin", NectarLogin::new);
conn.registerFunction("NectarAddPost", NectarAddPost::new);
conn.registerFunction("NectarGetPosts", NectarGetPosts::new);

ApiaryWorker apiaryWorker = new ApiaryWorker(conn, new ApiaryNaiveScheduler(), 4, "postgres", ApiaryConfig.provenanceDefaultAddress);
apiaryWorker.startServing();
```

After a worker is running, you can launch clients to communicate with it
and call functions.  For example, in a social network application,
to retrieve all messages sent to a user:

```java
ApiaryWorkerClient client = new ApiaryWorkerClient("localhost");
String[] posts = client.executeFunction("GetMessages", username).getStringArray();
```

### Provenance

Apiary captures _data provenance_ information on all function executions
and all operations functions perform on data. This information is stored
in database tables (by default in Postgres, but we also support Vertica)
for easy querying.  First, Apiary maintains a `FuncInvocations`
table storing information on all function invocations.  Its schema is:

| Field     | Type    | Description                           |
|-----------|---------|---------------------------------------|
| APIARY_TRANSACTION_ID    | BIGINT  | Postgres Transaction ID     .         |
| APIARY_TIMESTAMP | BIGINT  | Unix epoch timestamp in microseconds. |
| EXECUTIONID | BIGINT  | Unique ID of program execution.       |
| SERVICE | VARCHAR | Name of program service.              |
| PROCEDURENAME | VARCHAR | Function name.                        |

Then, for each database table in an application, Apiary
maintains an `Events` table tracking all operations that occured on the table:

| Field                 | Type   | Description                                                                          |
|-----------------------|--------|--------------------------------------------------------------------------------------|
| APIARY_TRANSACTION_ID | BIGINT | Postgres Transaction ID     .                                                        |
| APIARY_TIMESTAMP      | BIGINT | Unix epoch timestamp in microseconds.                                                |
| APIARY_OPERATION_TYPE | BIGINT | The type of the operation: 1 for inserts, 2 for deletes, 3 for updates, 4 for reads. |
| Table columns...      | Any    | The column values of the record being operated on.                                   |

### Further Reading 

If you haven't already, please look at the [tutorial](https://github.com/DBOS-project/apiary/blob/main/postgres-demo/README.md)
and documentation.  If you have any questions, feel free to contact us.