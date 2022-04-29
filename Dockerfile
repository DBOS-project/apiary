FROM ubuntu:18.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update
RUN apt install openjdk-11-jdk -y
RUN apt install git python maven curl -y
RUN apt install libatomic1 -y
ENV VOLTVERSION="voltdb-opensource-9.3.2"
RUN curl -LJO https://storage.googleapis.com/apiary_public/"${VOLTVERSION}".tar.gz

RUN tar -xzf "${VOLTVERSION}".tar.gz
ENV VOLT_HOME=/"${VOLTVERSION}"/
ENV PATH="${VOLT_HOME}/bin:${PATH}"
EXPOSE 21211/tcp 21212/tcp 22/tcp

RUN mkdir -p /apiary
ADD . /apiary
WORKDIR "/apiary/scripts"

ENTRYPOINT ./volt_docker_entry.sh
