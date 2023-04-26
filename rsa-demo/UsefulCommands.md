## Useful Commands for Demo

To change the prompt of the bash terminal, run:

```shell
export PS1="bash>"
```

To change the prompt of Vertica console, first connect to Vertica Docker:
```shell
docker exec -it vertica_ce /opt/vertica/bin/vsql
```

Then set the Vertica SQL prompts (both PROMPT1 and PROMPT2 for multiple lines):
```sql
\set PROMPT1 'dbadmin-> '
\set PROMPT2 'dbadmin-> '
```

You can clear the Vertica screen using:
```sql
\!clear
```
