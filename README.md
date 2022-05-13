<img src="https://storage.googleapis.com/apiary_public/apiary_logo_timeburner.png" width="400">

Welcome to Apiary!  Apiary is a transactional function-as-a-service (FaaS) framework for building
database-oriented applications such as microservices and web service backends.
Apiary provides an easy-to-use Java interface that supports general computation
and offers excellent performance, strong consistency guarantees,
and powerful new observability features such as automatic data provenance capture.
Apiary is an ongoing research project within the MIT-Stanford 
[DBOS](https://dbos-project.github.io/) collaboration;
we are interested in any feedback you can provide to improve the project
so it better meets developers' needs.

Apiary is based on two ideas that we believe can radically simplify many applications:

1. Store all long-lived application state in a database.
2. Access this state only through database transactions.

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

Apiary currently supports two database backends, Postgres and VoltDB,
although we're open to supporting more in the future.

### Getting Started

To get you started, we've written 
[a tutorial](postgres-demo/) showing you how to build a simple social network
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