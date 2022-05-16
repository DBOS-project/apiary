<img src="https://storage.googleapis.com/apiary_public/apiary_logo_timeburner.png" width="400">

Welcome to Apiary!  Apiary is a transactional function-as-a-service (FaaS) framework for building
database-oriented applications such as microservices and web service backends.
Apiary provides an easy-to-use Java interface that supports general computation
and offers excellent performance, strong consistency guarantees,
and powerful new observability features such as automatic data provenance capture.
Apiary is an ongoing research project within the MIT-Stanford 
[DBOS](https://dbos-project.github.io/) collaboration; it is licensed under the [MIT License](LICENSE).
We are interested in any feedback you can provide to improve the project
so it better meets developers' needs.

An Apiary application is composed of _transactional functions_,
which are regular Java functions that use SQL to access application state in the database,
but which execute as ACID database transactions.
We provide interfaces to write these functions, compose them into larger programs,
then schedule and run them.  Apiary provides three exciting features:

* High performance, especially in a distributed setting,
through aggressive co-location of compute and data.
* Strong consistency for programs, including exactly-once semantics for program execution 
and cross-function or even cross-application transactional guarantees.
* Data provenance capture for observability: we automatically record 
every function execution and every operation performed on data,
then store this information in easy-to query database tables to aid in
debugging, monitoring, and auditing.

Apiary currently supports two database backends: Postgres and VoltDB, and the provenance data is exported to either Postgres or Vertica for analysis.
We're open to supporting more in the future.

### Getting Started

To get you started, we provide a demo application: a simple [social network](postgres-demo/). We tested it under Ubuntu 18.04 and WSL2 on Windows 10.

Let's first install some dependencies:
```
sudo apt install openjdk-11-jdk maven libatomic1
```

Next, let's compile Apiary. In the Apiary root directory, run:
```
mvn -DskipTests package
```

Then, let's start Postgres from a Docker image:
```
scripts/initialize_postgres_docker.sh
```

To start the website, run in the `postgres-demo` root directory:

    mvn clean && mvn package && mvn spring-boot:run

Then, navigate to `localhost:8081` to view this new social network! You should see the Nectar homepage:
<img src="https://storage.googleapis.com/apiary_public/nectar_network_homepage.png" width="600">

To further understand Apiary, we've written 
[a detailed tutorial](postgres-demo/README.md) showing you how to build this social network
application with Apiary and [Spring Boot](https://spring.io/projects/spring-boot).
We also have a [programming guide](ProgrammingGuide.md)
for Apiary as well as [documentation](https://dbos-project.github.io/apiary-docs/).

### Contact Us

We're interested in any feedback you have to make this project better.
Apiary is primarily developed by [Peter Kraft](http://petereliaskraft.net/)
and [Qian Li](https://cs.stanford.edu/people/qianli/)
as part of the [DBOS](https://dbos-project.github.io/) project.
Contact us via email at:

    apiary-group@cs.stanford.edu