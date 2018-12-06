file(GLOB HEADERS "include/eosio/ledger_plugin/*.hpp")
include_directories(${CMAKE_CURRENT_SOURCE_DIR} include mysqlconn db /usr/include/mysql)
link_directories(/usr/local/lib /usr/lib)

add_library( ledger_plugin
            mysqlconn/mysqlconn.cpp
            db/connection_pool.cpp
            db/ledger_table.cpp
            ledger_plugin.cpp
            ${HEADERS} )

target_link_libraries( ledger_plugin 
    PUBLIC chain_plugin eosio_chain appbase
    mysqlclient z
)
target_include_directories( ledger_plugin PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )
eosio_additional_plugin(ledger_plugin)
