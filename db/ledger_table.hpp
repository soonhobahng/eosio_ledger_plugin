#ifndef LEDGER_H
#define LEDGER_H

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include "connection_pool.h"

namespace eosio {
    class ledger_table {
        public:
            ledger_table(std::shared_ptr<connection_pool> pool, uint32_t bulk_max_count);
            ~ledger_table();

            void add_ledger(uint64_t action_id, chain::transaction_id_type transaction_id, uint64_t block_number, std::string receiver, chain::action action);

            void finalize();
        private:
            std::shared_ptr<connection_pool> m_pool;

            uint32_t _raw_bulk_max_count;
            uint32_t _account_bulk_max_count;
            
            uint32_t raw_bulk_count = 0;
            std::ostringstream raw_bulk_sql;

            uint32_t account_bulk_count = 0;
            std::ostringstream account_bulk_sql;
    };
}
#endif