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
posts your friends sent you.  The first thing we need to do is
create some database tables in Postgres to store login information and posts.
We'll automatically create these tables when our web server starts.
We provide a special API for creating tables in Apiary, which uses
conventional Postgres syntax but also automatically records some metadata:

```java
PostgresConnection conn = new PostgresConnection("localhost", ApiaryConfig.postgresPort);
conn.createTable("WebsiteLogins", "Username VARCHAR(1000) PRIMARY KEY NOT NULL, Password VARCHAR(1000) NOT NULL");
conn.createTable("WebsitePosts", "Sender VARCHAR(1000) NOT NULL, Receiver VARCHAR(1000) NOT NULL, PostText VARCHAR(10000) NOT NULL");
```