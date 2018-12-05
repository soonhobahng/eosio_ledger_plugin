#ifndef LEDGER_H
#define LEDGER_H

#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include "../dbconn/dbconn.hpp"

namespace eosio {
    class ledger_table {
        public:
            ledger_table(std::shared_ptr<dbconn> pool);
            ~ledger_table();

            add_ledger(uint64_t action_id, chain::transaction_id_type transaction_id, uint64_t block_number, std::string receiver, chain::action action);
        private:
            std::shared_ptr<dbconn> m_pool;
    };
}
#endif