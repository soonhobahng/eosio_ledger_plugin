file(GLOB HEADERS "include/eosio/ledger_plugin/*.hpp")
include_directories(${CMAKE_CURRENT_SOURCE_DIR} include ledger dbconn/include db )
link_directories(dbconn/lib64)

add_library( ledger_plugin
            dbconn/dbconn.cpp
            ledger_plugin.cpp
            ${HEADERS} )

target_link_libraries( ledger_plugin 
    PUBLIC chain_plugin eosio_chain appbase
)
target_include_directories( ledger_plugin PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" )
eosio_additional_plugin(ledger_plugin)