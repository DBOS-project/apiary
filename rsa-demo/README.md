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

Then, reset the database tables, pre-populate them, and take a backup of the base data:

```shell
java -jar target/demo-exec-fat-exec.jar -s resetTables
java -jar target/demo-exec-fat-exec.jar -s populateDatabase -numUsers NUMUSERS
sudo -u postgres scripts/backup.sh /var/tmp/rsa-demo.backup
```

Now, let's run the site:

```shell
mvn spring-boot:run
```

Log in to the site (default passwords are the same as the usernames) and make a post.  Then, delete everything:

```shell
java -jar target/demo-exec-fat-exec.jar -s deleteDatabase
```

Go into Vertica and get the replay startID and endID from RECORDEDINPUTS. The startID is the first ID in the table, the endID is the ID of the first delete command.

```shell
docker exec -it vertica_ce /opt/vertica/bin/vsql
```

Then, restore it:

```shell
sudo -u postgres scripts/restore.sh /var/tmp/rsa-demo.backup
java -jar target/demo-exec-fat-exec.jar -s replay -startId STARTID -endId ENDID
```

All posts, including any new ones you added, should now be visible!