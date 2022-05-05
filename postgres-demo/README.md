# DBOS-Website

Requires Java 11 and maven:

    apt install openjdk-11-jdk maven

To compile Apiary and start Postgres, in the parent directory, run:
```
mvn -DskipTests package
scripts/initialize_postgres_docker.sh
```

To compile and test:

    mvn clean package

To host the website locally:

    mvn spring-boot:run

The website is hosted at:

    localhost:8081
