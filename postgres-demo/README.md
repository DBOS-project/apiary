# Apiary Tutorial and Demo

This tutorial will show you how to build a simple social network
web application using Apiary and [Spring Boot](https://spring.io/projects/spring-boot).
To get started, let's first install some dependencies: 

    sudo apt install openjdk-11-jdk maven libatomic1

Next, let's compile Apiary. In the Apiary root directory, run:

    mvn -DskipTests package

Then, let's start Postgres from a Docker image:

    scripts/initialize_postgres_docker.sh

Now, it's time to build a website!
We want to build a simple social network application where you can
register, log in, send posts to your friends, and read
posts your friends sent you.  Let's call this site the Nectar Network.
We'll build the site using [Spring Boot](https://spring.io/projects/spring-boot),
but call Apiary functions to handle operations on website data,
like registering users or adding new posts.
Then, once the website is running,
we'll show how Apiary's new data provenance features make it easy
to monitor website activity and provide
cool features like easily rolling back
your database and application to any previous point in time.

### Tables 
The first thing we need to do is  create some database tables in Postgres
to store the information our site needs: logins and posts.
We create these tables inside the Spring Boot controller
when our web server starts;
the full code for it is [here](https://github.com/DBOS-project/apiary/blob/main/postgres-demo/src/main/java/org/dbos/apiary/postgresdemo/NectarController.java).
We provide an API for creating tables in Apiary, which uses
conventional Postgres syntax:

```java
PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort);
conn.createTable("WebsiteLogins", "Username VARCHAR(1000) PRIMARY KEY NOT NULL, Password VARCHAR(1000) NOT NULL");
conn.createTable("WebsitePosts", "Sender VARCHAR(1000) NOT NULL, Receiver VARCHAR(1000) NOT NULL, PostText VARCHAR(10000) NOT NULL");
```

### Functions

Now, let's write some functions.
We'll start with a simple register function that registers new users.
To write a function in Apiary using the Postgres backend,
we subclass the `PostgresFunction` class
and implement our function in its `runFunction` method.
Functions are written in regular Java with
embedded SQL and can take in and return
strings, integers, arrays of strings, and arrays of integers.
Here's what the register function looks like:

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

Every function has a context, which exposes the
Apiary API.  You'll use the context to talk to the database
and to call other functions.  Here, we use the context to execute
a couple of SQL queries: we first check if the username already exists,
fail if it does, and otherwise register a new user.
Every Apiary function runs as an ACID database transaction,
so once we've checked that a name isn't taken, it's impossible
for another function to take the name before our function completes.
Because this is a demo, we store passwords in plain text,
but please please please do not do this in production.

Back in Spring, we call the `register` function
whenever we get a registration request to our site:
```java
@PostMapping("/registration")
public String registrationSubmit(@ModelAttribute Credentials credentials, Model model) throws IOException {
    int success = client.executeFunction("localhost", "NectarRegister", "NectarNetwork", credentials.getUsername(), credentials.getPassword()).getInt();
    if (success != 0) {
        return "redirect:/home";
    }
    model.addAttribute("registration", credentials);
    return "registration_result";
}
```

Now, let's write a `login` function that actually logs a user in.
It looks similar to `register`:

```java
public class NectarLogin extends PostgresFunction {

    private static final String checkPassword = "SELECT Username, Password FROM WebsiteLogins WHERE Username=?";

    public static int runFunction(ApiaryTransactionalContext ctxt, String username, String password) throws SQLException {
        ResultSet pwdCheck = (ResultSet) ctxt.apiaryExecuteQuery(checkPassword, username);
        if (pwdCheck.next() && pwdCheck.getString(2).equals(password)) {
            return 0; // Success!
        } else {
            return 1; // Failed login: the user does not exist or the password is wrong.
        }
    }
}
```

In Spring, we call this function whenever a user tries to log in:

```java
@PostMapping("/login")
public RedirectView loginSubmit(@ModelAttribute Credentials credentials, @ModelAttribute("logincredentials") Credentials logincredentials, RedirectAttributes attributes) throws InvalidProtocolBufferException {
    int success = client.executeFunction("localhost", "NectarLogin", "NectarNetwork", credentials.getUsername(), credentials.getPassword()).getInt();
    if (success == 0) { // Login successful.
        logincredentials.setUsername(credentials.getUsername());
        logincredentials.setPassword(credentials.getPassword());
        // Ensure credentials are saved across page reloads.
        attributes.addFlashAttribute("logincredentials", logincredentials);
        return new RedirectView("/timeline");
    } else { // Login failed.
        return new RedirectView("/home");
    }
}
```

We similarly write [AddPosts](https://github.com/DBOS-project/apiary/blob/main/postgres-demo/src/main/java/org/dbos/apiary/postgresdemo/functions/NectarAddPost.java)
and [GetPosts](https://github.com/DBOS-project/apiary/blob/main/postgres-demo/src/main/java/org/dbos/apiary/postgresdemo/functions/NectarGetPosts.java)
functions in Apiary and call them in Spring;
you can see code for all four functions [here](https://github.com/DBOS-project/apiary/tree/main/postgres-demo/src/main/java/org/dbos/apiary/postgresdemo/functions).

### Tying it Together

With our functions written, it's almost time to launch our site.
We'll now tell the [Spring controller](https://github.com/DBOS-project/apiary/blob/main/postgres-demo/src/main/java/org/dbos/apiary/postgresdemo/NectarController.java)
to launch an Apiary worker on startup to manage all the Apiary function requests,
then register all our functions with the worker:

```java
PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort);
conn.registerFunction("NectarRegister", NectarRegister::new);
conn.registerFunction("NectarLogin", NectarLogin::new);
conn.registerFunction("NectarAddPost", NectarAddPost::new);
conn.registerFunction("NectarGetPosts", NectarGetPosts::new);

ApiaryWorker apiaryWorker = new ApiaryWorker(conn, new ApiaryNaiveScheduler(), 4);
apiaryWorker.startServing();
```

Everything's ready!  To start the site, run in the `postgres-demo` root directory:

    mvn clean && mvn package && mvn spring-boot:run

Then, navigate to `localhost:8081` to view this new social network!

### Provenance

