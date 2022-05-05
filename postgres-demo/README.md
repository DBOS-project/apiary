# DBOS-Website

Requires Java 11 and maven:

    apt install openjdk-11-jdk maven

Also depends on DBOS, which must be located and compiled in a sibling directory (TODO:  Fix this, probably with a submodule).

With a VoltDB cluster initialized, run:

    scripts/create_tables.sh
    scripts/update_website_procedures.sh

To compile and test:

    mvn clean package

To host the website locally:

    mvn spring-boot:run

The website is hosted at:

    localhost:8081/home
    
To see this is working, register a user, login, and create several posts. Then go to the VoltDB console (localhost:8080) or use `sqlcmd` and run:

    SELECT * FROM WebsiteLogins;
    
    SELECT * FROM WebsitePosts;

You should be able to see your account and posts you created.

With local provenance (read/write/metadata) enabled, you should be able to find read logs (csv files) under `/var/tmp/voltdbroot`, and write logs (csv files) under `/var/tmp/voltdbroot/cdctest`.
In the future, all provenance data will be stored in Vertica.

## Deploy on GCP
1. Create a VM instance on GCP that starts VoltDB and compiles DBOS code. You could use the TF scripts here in the `website` branch: https://github.com/DBOS-project/terraform-scripts/tree/website
2. Then ssh into the VM you just created and make sure the VoltDB cluster is up. For example, check the `/output.log` and make sure everything is properly started:
    ```
    > tail -f /output.log

    DROP PROCEDURE WebsitePost IF EXISTS;
    Command succeeded.

    CREATE PROCEDURE PARTITION ON TABLE WebsitePosts COLUMN PKey PARAMETER 0 FROM CLASS dbos.procedures.WebsitePost;
    Command succeeded.
    Total host count: 1
    All VoltDB hosts: dbos-qian-web2-voltdb-main-0
    ==== Finished VoltDB metadata startup script. ====
    12312021 22:21:51 : Done, you can now exit log file and continue working
    ```
3. Follow the above instruction to compile and run the website. If you use the TF scripts, this repo should be located under `/dbos-website`. For example:
   ```
   cd /dbos-website
   mvn clean
   mvn package
   mvn spring-boot:run
   ```
4. Reserve a static external IP address and link to the VM instance you just created. Follow the instruction here: https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address
5. Test if you could access the website through `<external-ip>:8081/home`. If not, you'll need to set the firewall rule. To find firewall rules, go to https://console.cloud.google.com/networking/firewalls/list
    ```
    Edit external-firewall-dbos-<your name>-voltdb-main-0
    Source filter: change to IPv4 ranges.
    Source IPv4 ranges: 0.0.0.0/0  -> so that everyone can access it.
    Specified protocols and ports: tcp: 8081  -> the port should be the same as the website port.
    ```
