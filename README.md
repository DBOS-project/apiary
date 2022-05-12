<img src="https://storage.googleapis.com/apiary_public/apiary_logo_timeburner.png" width="400">

Welcome to Apiary!  Apiary is a transactional function-as-a-service (FaaS) framework for building
database-oriented applications such as microservices and web service backends.
Apiary provides an easy-to-use Java interface that supports general computation
and offers good performance, strong consistency guarantees,
and powerful new observability features such as automatic data provenance tracking.
Apiary is an ongoing research project within the larger MIT-Stanford 
[DBOS](https://dbos-project.github.io/) collaboration;
we are interested in any feedback you can provide to improve the project
so it better meets developers' needs.

The idea behind Apiary is that you store all your application's long-lived state in a database
(which you were likely already doing),
then we run every function in your application as a database transaction.
You write your functions in regular Java, using SQL to talk to the database,
and we schedule and run these functions, providing three exciting features:

* Advanced observability capabilities by combining data provenance information recorded in the database with control flow information captured by functions--we automatically record anything your application ever does in easy-to-query database tables.
* Improved performance in a distributed setting through aggressive co-location of compute and data.
* Strong consistency for programs, including easy cross-function and even cross-application transactional guarantees.

Apiary currently supports two database backends, Postgres and VoltDB, although we're open to
supporting more in the future.

To get you started, we've written 
[a tutorial](https://github.com/DBOS-project/apiary/tree/main/postgres-demo) showing you how to build a simple social network
application with Apiary and [Spring Boot](https://spring.io/projects/spring-boot).
