#include "ledger_table.hpp"
#include "mysqlconn.h"

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>
#include <eosio/chain/block_timestamp.hpp>

#include <eosio/chain_plugin/chain_plugin.hpp>

#include <boost/chrono.hpp>
#include <boost/format.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <iostream>
#include <future>

namespace eosio {

extern void post_query_str_to_queue(const std::string query_str);

static const std::string LEDGER_INSERT_STR =
    "INSERT IGNORE INTO ledger(action_id, transaction_id, block_number, timestamp, contract_owner, from_account, to_account, amount, precision, symbol, receiver, action_name, created_at ) VALUES ";
static const std::string ACTIONS_ACCOUNT_INSERT_STR = 
    "INSERT INTO actions_accounts(action_id, actor, permission) VALUES ";

ledger_table::ledger_table(std::shared_ptr<connection_pool> pool, uint32_t raw_bulk_max_count, uint32_t account_bulk_max_count) :
m_pool(pool), _raw_bulk_max_count(raw_bulk_max_count), _account_bulk_max_count(account_bulk_max_count)
{

}

ledger_table::~ledger_table()
{

}

void ledger_table::add_ledger(uint64_t action_id, chain::transaction_id_type transaction_id, uint64_t block_number, chain::block_timestamp_type block_time, std::string receiver, chain::action action) 
{
    chain::abi_def abi;
    std::string abi_def_account;
    chain::abi_serializer abis;
    
    const auto transaction_id_str = transaction_id.str();
    const auto block_num = block_number;
    const auto block_timestamp = std::chrono::seconds{block_time.operator fc::time_point().sec_since_epoch()}.count();
    string action_account_name = action.account.to_string();
    int max_field_size = 6500000;
    string escaped_json_str;
    string hex_str;

    string from_name;
    string to_name;
    int64_t asset_qty;
    int64_t precision;
    string symbol;

    try {
        try {  
            if (action.name == N(transfer)) {
                // get abi definition from chain
                chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
                EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
                auto& db = chain_plug->chain();
                chain::abi_def abi_chain = db.db().find<chain::account_object, chain::by_name>(action.account)->get_abi();    

                if(!abi_chain.version.empty()){
                    abi = abi_chain;
                    string abi_json = fc::json::to_string(abi);

                    static const fc::microseconds abi_serializer_max_time(1000000); // 1 second
                    abis.set_abi(abi, abi_serializer_max_time);
                    auto abi_data = abis.binary_to_variant(abis.get_action_type(action.name), action.data, abi_serializer_max_time);

                    from_name = abi_data["from"].as<chain::name>().to_string();
                    to_name = abi_data["to"].as<chain::name>().to_string();
                    
                    if(from_name != receiver) return;

                    auto asset_quantity = abi_data["quantity"].as<chain::asset>();
                    asset_qty = asset_quantity.get_amount();
                    precision = asset_quantity.precision();

                    ilog("amount : ${a}, precision : ${p}",("a",asset_qty)("p",precision));
                    
                    // asset_qty = asset_quantity.to_real();

                    symbol = asset_quantity.get_symbol().name();

                    std::ostringstream raw_bulk_sql_add;
                    std::ostringstream raw_bulk_sql_sub;

                    raw_bulk_sql_add << boost::format("INSERT INTO tokens (account, amount, symbol, precision) VALUES ('%1%', '%2%', '%3%', '%4%') ON DUPLICATE KEY UPDATE amount = amount + %2% ;")
                    % to_name
                    % asset_qty
                    % symbol
                    % precision;

                    raw_bulk_sql_sub << boost::format("INSERT INTO tokens (account, amount, symbol, precision) VALUES ('%1%', '-%2%', '%3%', '%4%') ON DUPLICATE KEY UPDATE amount = amount - %2% ;")
                    % from_name
                    % asset_qty
                    % symbol
                    % precision;

                    shared_ptr<MysqlConnection> con = m_pool->get_connection();
                    assert(con);
                    try{
                            con->execute(raw_bulk_sql_add.str(), true);
                            con->execute(raw_bulk_sql_sub.str(), true);

                            m_pool->release_connection(*con);
                    } catch (...) {
                            m_pool->release_connection(*con);
                    }                    
                } else if (action.account == chain::config::system_account_name) {
                    abi = chain::eosio_contract_abi(abi); 
                } else {
                    return;         // no ABI no party. Should we still store it?
                }
            } else {
                return;
            }
                
        } catch( std::exception& e ) {
            // ilog( "Unable to convert action.data to ABI: ${s}::${n}, std what: ${e}",
            //       ("s", action.account)( "n", action.name )( "e", e.what()));
            return;
        } catch (fc::exception& e) {
            // if (action.name != "onblock") { // eosio::onblock not in original eosio.system abi
            //     ilog( "Unable to convert action.data to ABI: ${s}::${n}, fc exception: ${e}",
            //         ("s", action.account)( "n", action.name )( "e", e.to_detail_string()));
            // }
            return;
        } catch( ... ) {
            // ilog( "Unable to convert action.data to ABI: ${s}::${n}, unknown exception",
            //       ("s", action.account)( "n", action.name ));
            return;
        }

        // ledger 테이블 인서트. 
        {
            if (raw_bulk_count > 0) {
                raw_bulk_sql << ", ";
            }


            raw_bulk_sql << boost::format("('%1%', '%2%', '%3%', FROM_UNIXTIME('%4%'), '%5%', '%6%', '%7%', '%8%', '%9%', '%10%', '%11%', '%12%', CURRENT_TIMESTAMP)")
                % action_id
                % transaction_id_str
                % block_num
                % block_timestamp
                % action_account_name
                % from_name
                % to_name
                % asset_qty
                % precision
                % symbol
                % receiver
                % action.name.to_string();

            raw_bulk_count++;

            if (raw_bulk_count >= _raw_bulk_max_count)
                post_raw_query();
        }

        // action_account 테이블 인서트
        for (const auto& auth : action.authorization) {
            if (account_bulk_count > 0) {
                account_bulk_sql << ", ";
            }

            account_bulk_sql << boost::format(" ('%1%','%2%','%3%')") 
                % action_id 
                % auth.actor.to_string()
                % auth.permission.to_string();

            account_bulk_count++;

            if (account_bulk_count >= _account_bulk_max_count) 
                post_acc_query();

        }

    } catch( fc::exception& e ) {
        wlog(e.what());
    }    

}

void ledger_table::finalize() {
    post_raw_query();
    post_acc_query();
}

void ledger_table::post_raw_query() {
    if (raw_bulk_count) {
        post_query_str_to_queue(
            LEDGER_INSERT_STR +
            raw_bulk_sql.str()
        ); 

        raw_bulk_sql.str(""); raw_bulk_sql.clear(); 
        raw_bulk_count = 0; 
    }

}

void ledger_table::post_acc_query() {
    if (account_bulk_count) {
        post_query_str_to_queue(
            ACTIONS_ACCOUNT_INSERT_STR +
            account_bulk_sql.str()
        ); 

        account_bulk_sql.str(""); account_bulk_sql.clear(); 
        account_bulk_count = 0; 
    }
}

}