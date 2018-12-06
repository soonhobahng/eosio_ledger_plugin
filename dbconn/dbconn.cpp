#include "dbconn.hpp"

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <iostream>
#include <mysqlx/xdevapi.h>

using std::string;

namespace eosio {
    

    dbconn::dbconn(const std::string host, const std::string user, const std::string passwd, const std::string database, const uint16_t port, const uint16_t max_conn)
    {


        std::ostringstream conn_str;
        conn_str << boost::format("'%1%':'%2%'@'%3%':'%5%'/'%4%'")
        % user
        % passwd
        % host
        % database
        % port;

        const char* conn_char = conn_str.str().c_str();

        mysqlx.Client cli_tmp(conn_char,mysqlx.ClientOption::POOL_MAX_SIZE,max_conn);   
        cli = &cli_tmp;
    }

    dbconn::~dbconn()
    {
        cli->close();
    }

    mysqlx.Session dbconn::get_connection()
    {
        return cli->getSession();
    }
}
