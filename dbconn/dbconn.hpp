#ifndef DBCONN_H
#define DBCONN_H

#pragma once

#include <iostream>
#include <string>
#include <memory>

#include <mysqlx/xdevapi.h>

using std::shared_ptr;

namespace eosio{
class dbconn {
    public:
        explicit dbconn(const std::string host, const std::string user, const std::string passwd, const std::string database, const uint16_t port, const uint16_t max_conn);
        ~dbconn();

        mysqlx.Session get_connection();

    private:
        mysqlx.Client *cli;


};
}

#endif