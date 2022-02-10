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