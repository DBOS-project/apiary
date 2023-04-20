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

Next, compile the simple social network site.  In the `rsa-demo` root directory, run:

```shell
mvn clean && mvn package
```

Then, reset the database tables and pre-populate them:

```shell
java -jar target/demo-exec-fat-exec.jar -s resetTables
java -jar target/demo-exec-fat-exec.jar -s populateDatabase
```

Now, let's run the site:

```shell
mvn spring-boot:run
```

You can visit the site at `http://<ip-address>:80` and try to log in with some generated names such as "Michael Stonebraker".
You will be able to send posts to others, and read posts sent from others.

To exfiltrate the site's data to a file, run this script:

```shell
scripts/exfiltrate.sh "<User Name>"
```

This exfiltration should be detected and Apiary would suspend the compromised account. If you run the script again, it should show an error message saying the account has been suspended.
Then, to find all users whose posts were exfiltrated, run this SQL query in Vertica:

```sql
SELECT DISTINCT p.receiver
FROM FuncInvocations f JOIN WebsitePostsEvents p ON f.apiary_transaction_id = p.apiary_transaction_id 
WHERE f.apiary_role = 'admin_2' AND f.apiary_procedurename = 'NectarGetPosts' 
ORDER BY p.receiver;
```

The users returned by this query should match the users whose data is exfiltrated before the compromised account was suspended.
