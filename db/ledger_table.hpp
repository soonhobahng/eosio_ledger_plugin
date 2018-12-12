#ifndef LEDGER_H
#define LEDGER_H

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>
#include <eosio/chain/block_timestamp.hpp>

#include "connection_pool.h"

namespace eosio {
    class ledger_table {
        public:
            ledger_table(std::shared_ptr<connection_pool> pool, uint32_t raw_bulk_max_count, uint32_t account_bulk_max_count);
            ~ledger_table();

            void add_ledger(uint64_t action_id, chain::transaction_id_type transaction_id, uint64_t block_number, chain::block_timestamp_type block_time, std::string receiver, chain::action action);

            void finalize();

            void tick(const int64_t tick);
        private:
            void post_raw_query();
            void post_acc_query();

            std::shared_ptr<connection_pool> m_pool;

            uint32_t _raw_bulk_max_count;
            uint32_t _account_bulk_max_count;

            uint32_t raw_bulk_count = 0;
            int64_t raw_bulk_insert_tick = 0;
            std::ostringstream raw_bulk_sql;
            std::string str_raw_bulk_sql;

            uint32_t account_bulk_count = 0;
            int64_t account_bulk_insert_tick = 0;
            std::ostringstream account_bulk_sql;
            std::string str_account_bulk_sql;
    };
}
#endif