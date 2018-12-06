#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <iostream>

#include "dbconn.hpp"
#include "include/mysqlx/xapi.h"

using std::string;

namespace eosio {
    dbconn::dbconn(const std::string host, const std::string user, const std::string passwd, const std::string database, const uint16_t port, const uint16_t max_conn)
    {
        char error_buf[255];
        int  error_code;

        std::ostringstream conn_str;
        conn_str << boost::format("'%1%':'%2%'@'%3%':'%5%'/'%4%'")
        % user
        % passwd
        % host
        % database
        % port;

        const char* conn_char = conn_str.str().c_str();
        char* c_max_size = nullptr;
  
        sprintf(c_max_size,"{ \"maxSize\": %d }",max_conn);
        cli = mysqlx_get_client_from_url( conn_char, c_max_size, error_buf, &error_code);
    
    }

    dbconn::~dbconn()
    {
        mysqlx_client_close(cli);
    }

    mysqlx_session_t* dbconn::get_connection()
    {
        char error_buf[255];
        int  error_code;

        mysqlx_session_t *sess = mysqlx_get_session_from_client(cli, error_buf, &error_code);

        return sess;
    }

    void dbconn::release_connection(mysqlx_session_t* sess)
    {
        mysqlx_session_close(sess);  // close session sess
    }
}
