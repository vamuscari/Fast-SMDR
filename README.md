# Fast SMDR

## Name
  Fast-SMDR - listens for TCP input from an Avaya system

## SYNOPSIS
  Fast-SMDR [-p port] [-f filter] [-d database_connection] 


-p <port> : The TCP port that the service will listen on. Default is 514

-f <filter> : Filter incoming entries. Not required but recommended

-d  <database_connection> : The connection string for the pg db


## Examples

```
Fast-SMDR -p 514 -f 10.1.1.100 -d "postgres://pgx_md5:secret@localhost:5432/avaya?sslmode=disable"
```
