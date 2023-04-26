# RSA Demo

This is code for a demo of Apiary/DBOS that was presented at RSA 2023.  It is based on our [Apiary-Postgres demo](https://github.com/DBOS-project/apiary/tree/main/postgres-demo).

To run it, let's first install some dependencies:

```shell
sudo apt install openjdk-11-jdk maven libatomic1
```

You also must have Docker installed.

Next, let's compile Apiary. In the Apiary root directory, run:

```shell
mvn -DskipTests package
```

Then, let's start Postgres and Vertica from Docker images. We recommend you [configure Docker](https://docs.docker.com/engine/install/linux-postinstall/) so it can be run by non-root users.

```shell
scripts/initialize_postgres_docker.sh
scripts/initialize_vertica_docker.sh
```

Next, compile the demo site.  In the `rsa-demo` root directory, run:

```shell
mvn clean && mvn package
```

Then, reset the database tables and pre-populate them with pre-generated users and posts. The default password for every user is "test".

```shell
java -jar target/demo-exec-fat-exec.jar -s resetTables
java -jar target/demo-exec-fat-exec.jar -s populateDatabase
```

Now, let's run the site:

```shell
mvn spring-boot:run
```

You can visit the site at `http://<ip-address>:80`.

To simulate a compromised admin account exfiltrating user data, run:

```shell
scripts/exfiltrate.sh "<User Name>"
```

When prompted, press the `Enter` key.  

Apiary detects the simulated exfiltration and suspends the account associated with the exfiltration script.
If you run the script again, it should show an error message saying the account has been suspended.
Then, to find all users whose posts were exfiltrated, run this SQL query in Vertica:

```shell
docker exec -it vertica_ce /opt/vertica/bin/vsql
```

```sql
SELECT DISTINCT p.receiver
FROM FuncInvocations f JOIN WebsitePostsEvents p ON f.apiary_transaction_id = p.apiary_transaction_id 
WHERE f.apiary_role = 'admin_2' AND f.apiary_procedurename = 'NectarGetPosts' 
ORDER BY p.receiver;
```

The users returned by this query should match the users whose data was exfiltrated before the compromised account was suspended.
