## R<sup>3</sup>:Record-Replay-Retroaction (VLDB'23)

This document contains instructions to run experiments in our VLDB'23 paper:

**Qian Li**, Peter Kraft, Michael Cafarella, Çağatay Demiralp ,Goetz Graefe, Christos Kozyrakis, Michael Stonebraker, Lalith Suresh, Xiangyao Yu, and Matei Zaharia

[R<sup>3</sup>: Record-Replay-Retroaction for Database-Backed Applications](#)

*Proc. of the VLDB Endowment (PVLDB), Volume 16, Vancouver, BC, Canada, 2023.*

Most record/replay/retroaction code can be found in the following files:
- [ProvenanceBuffer.java](src/main/java/org/dbos/apiary/function/ProvenanceBuffer.java)
- [PostgresRetroReplay.java](src/main/java/org/dbos/apiary/postgres/PostgresRetroReplay.java)
- [PostgresContext.java](src/main/java/org/dbos/apiary/postgres/PostgresContext.java)
- [PostgresConnection.java](src/main/java/org/dbos/apiary/postgres/PostgresConnection.java)

You can find benchmark source code in the following places:
- [MoodleBenchmark.java](src/main/java/org/dbos/apiary/benchmarks/retro/MoodleBenchmark.java)
- [WordPressBenchmark.java](src/main/java/org/dbos/apiary/benchmarks/retro/WordPressBenchmark.java)
- [TPC-C](src/main/java/org/dbos/apiary/benchmarks/tpcc)

### Preparation

Let's first install some dependencies:

```shell
sudo apt install openjdk-11-jdk maven libatomic1
```

Then, download this repository:
```shell
git clone https://github.com/DBOS-project/apiary.git
git checkout r3-exp
```

Next, let's compile Apiary. In the Apiary root directory, run:

```shell
mvn -DskipTests package
```

Then, let's start Postgres from a Docker image. We recommend you [configure Docker](https://docs.docker.com/engine/install/linux-postinstall/) so it can be run by non-root users.

```shell
scripts/initialize_postgres_docker.sh
```

We use Vertica as the data recorder. For convenience, ye can also run it in a Docker:

```shell
scripts/initialize_vertica_docker.sh
```

### Running Experiments

To execute workloads with R<sup>3</sup> capturing:
```shell
java -jar target/apiary-bench-exec-fat-exec.jar -b retro -s <workload: moodle, wordpress, tpcc> -notxn -d <duration: 60 sec> -p1 <read ratio: 90> -p2 <write ratio: 10> -retroMode 0 -bugfix none -mainHostAddr <postgres server address> -i <request arrival interval: 2000 us>
```

To execute the `no-record` baseline, you can simply add `-noProv` to the above command. This baseline does not record any data to the recorder.

Then, you can replay the recorded trace (from the command you just executed) and test the replay performance:
```shell
java -jar target/apiary-bench-exec-fat-exec.jar -b retro -s <workload: moodle, wordpress, tpcc> -notxn  -retroMode 2 -bugFix none -execId <the first request ID: query it from the data recorder> -mainHostAddr <postgres server address>
```

To test the performance of the `sequential` baseline, you can simply add `-seqReplay` to the above command and re-execute.

After that, for Moodle and WordPress, you can replay the recorded trace with bugfixed code. You need to specify which bugfix you want to use:
```shell
java -jar target/apiary-bench-exec-fat-exec.jar -b retro -s <workload: moodle, wordpress, tpcc> -notxn  -retroMode 2 -bugFix <moodle: subscribe, wordpress: comment, option> -execId <the first request ID: query it from the data recorder> -mainHostAddr <postgres server address>
```

Finally, you can test the performance of our selective retroactive execution, by simply specifying `-retroMode 3` in the above command.
