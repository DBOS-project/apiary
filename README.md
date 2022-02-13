# Dynamic Apiary
Requires Java 11 and maven:

    apt install openjdk-11-jdk maven

Also requires VoltDB; set the following environment variables:

    export VOLT_HOME=/path/to/voltdb
    export PATH="$VOLT_HOME/bin:$PATH"

To initialize VoltDB, run its startup script:

    scripts/initialize_voltdb.sh

To compile and run unit tests:

    mvn package

## Postgres

Tutorial for installing PostgreSQL and PL/Java that works on GCP Ubuntu machines.

### Installation and Simple Test
First, install and enable Postgres on your machine. Reference: https://wiki.postgresql.org/wiki/Apt
```
sudo apt install curl ca-certificates gnupg

curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null

sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'

sudo apt update
sudo apt install postgresql-14
sudo apt install postgresql-14-pljava  
```

Then, setup a role for your username in Postgres.
```
# Create a role for your username, so you can run psql command.
sudo -u postgres createuser --interactive

# Then, create a database for the username you just created.
sudo -u postgres createdb <usernmae>
```

Now you should be able to run `psql` from your user account.
Let's enable PLJava extension in psql.
```
psql

# Note that the libjvm.so path may differ on different machines.
SET pljava.libjvm_location TO '/usr/lib/jvm/java-1.11.0-openjdk-amd64/lib/server/libjvm.so';
CREATE EXTENSION pljava;

GRANT USAGE ON LANGUAGE java TO <username>;

ALTER DATABASE <username> SET pljava.libjvm_location FROM CURRENT;

# Create a test table and add some data.
CREATE TABLE KVTable(pkey integer NOT NULL, KVKey integer NOT NULL, KVValue integer NOT NULL);

INSERT INTO KVTable values (0, 0, 1);
```

Then, download PLJava and compile the api.
```
cd $HOME
git clone https://github.com/tada/pljava.git
cd pljava
git checkout V1_6_4

mvn clean install --> it's fine to fail. As long as it registers in maven local repo.
cd pljava/pljava-api
mvn install
```

Finally, register and run the sample function.
```
cd dynamic-apiary/
mvn -DskipTests package

psql

# Replace with the jar location.
SELECT sqlj.install_jar( 'file:/home/<user>/dynamic-apiary/target/dynamic-apiary-0.1-SNAPSHOT.jar','jfunctions', true );

SELECT sqlj.set_classpath('public', 'jfunctions');

SELECT greet('qian');

# Expected result:
         greet
-----------------------
 Hello World, qian 1 !
(1 row)
```
