# eosio_ledger_plugin

EOSIO plugin for collecting create/transfer actions data to MySQL.


## Requirements
- Works on any EOSIO node that runs v1.1.0 and up.
- On Ubuntu install the package libmysqlclient-dev
```
sudo apt install libmysqlclient-dev
```

- On OSX
```
brew install mysql-client
```

## Building the plugin 
### EOSIO v1.2.0 and up
```
-DEOSIO_ADDITIONAL_PLUGINS=<path-to-eosio-ledger-plugin>
```

```
$ nodeos --help

....
Config Options for eosio::ledger_plugin:
    --ledger-data-wipe = true                   if true, wipe all tables from database
    --ledger-queue-size  arg (=256)             The queue size between nodeos and MySQL 
                                                DB plugin thread.
    --ledger-db-host = arg                      MySQL DB host address.
                                                If not specified then plugin is disabled. 
                                                e.g. 127.0.0.1
    --ledger-db-port = <port no.>               port number e.g. 3306
    --ledger-db-user = <user name>
    --ledger-db-passwd = <password>
    --ledger-db-database = <database name>
    --ledger-db-max-connection = arg (=20)  max connection pool size.
....
```
