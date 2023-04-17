# RSA Demo

Let's first install some dependencies:

```shell
sudo apt install openjdk-11-jdk maven libatomic1
```

Next, let's compile Apiary. In the Apiary root directory, run:

```shell
mvn -DskipTests package
```

Then, let's start Postgres and Vertica from Docker images. We recommend you [configure Docker](https://docs.docker.com/engine/install/linux-postinstall/) so it can be run by non-root users.

```shell
scripts/initialize_postgres_docker.sh
scripts/initialize_vertica_docker.sh
```

Next, compile the site.  In the `rsa-demo` root directory, run:

```shell
mvn clean && mvn package
```

Then, reset the database tables and pre-populate them:

```shell
java -jar target/demo-exec-fat-exec.jar -s resetTables
java -jar target/demo-exec-fat-exec.jar -s populateDatabase -numUsers NUMUSERS
```

Now, let's run the site:

```shell
mvn spring-boot:run
```