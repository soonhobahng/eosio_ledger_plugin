/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/ledger_plugin/ledger_plugin.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include <queue>
#include <sstream>

#include <future>

#include "ledger_table.hpp"

namespace fc { class variant; }

namespace eosio {
using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::permission_name;
using chain::transaction;
using chain::signed_transaction;
using chain::signed_block;
using chain::transaction_id_type;
using chain::packed_transaction;

int queue_sleep_time = 0;
static appbase::abstract_plugin& _ledger_plugin = app().register_plugin<ledger_plugin>();

class ledger_plugin_impl;
static ledger_plugin_impl* static_ledger_plugin_impl = nullptr; 

class ledger_plugin_impl {
   public:
      ledger_plugin_impl();
      ~ledger_plugin_impl();

      fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;
      
      void consume_query_process();
      void consume_applied_transactions();

      void applied_transaction(const chain::transaction_trace_ptr&);
      void process_applied_transaction(const chain::transaction_trace_ptr&);
      void _process_applied_transaction(const chain::transaction_trace_ptr&);

      void process_add_ledger( const chain::action_trace& atrace );

      void init(const std::string host, const std::string user, const std::string passwd, const std::string database, 
         const uint16_t port, const uint16_t max_conn, bool do_close_on_unlock, uint32_t block_num_start, const variables_map& options);
      void wipe_database();

      template<typename Queue, typename Entry> void queue(Queue& queue, const Entry& e);

      bool configured{false};
      bool wipe_database_on_startup{false};
      uint32_t start_block_num = 0;
      uint32_t end_block_num = 0;
      bool start_block_reached = false;
      bool is_producer = false;

      std::deque<std::string> query_queue; 
      std::deque<chain::transaction_trace_ptr> transaction_trace_queue;

      boost::mutex mtx;
      boost::mutex mtx_applied_trans;
      boost::condition_variable condition;
      std::vector<boost::thread> consume_query_threads;
      std::vector<boost::thread> consume_applied_trans_threads;
      // boost::thread consume_thread_applied_trans;

      boost::atomic<bool> done{false};
      boost::atomic<bool> startup{true};
      fc::optional<chain::chain_id_type> chain_id;
      fc::microseconds abi_serializer_max_time;

      /**
       * database connection
       */
      std::shared_ptr<connection_pool> m_connection_pool;
      std::shared_ptr<ledger_table> m_ledger_table;
      std::string system_account;

      uint32_t m_block_num_start;
      size_t max_queue_size      = 100000; 
      size_t query_thread_count  = 4; 

};

template<typename Queue, typename Entry>
void ledger_plugin_impl::queue( Queue& queue, const Entry& e ) {
   boost::mutex::scoped_lock lock( mtx );
   auto queue_size = queue.size();
   if( queue_size > max_queue_size ) {
      lock.unlock();
      condition.notify_one();
      queue_sleep_time += 10;
      if( queue_sleep_time > 1000 )
         wlog("queue size: ${q}", ("q", queue_size));
      boost::this_thread::sleep_for( boost::chrono::milliseconds( queue_sleep_time ));
      lock.lock();
   } else {
      queue_sleep_time -= 10;
      if( queue_sleep_time < 0 ) queue_sleep_time = 0;
   }
   queue.emplace_back( e );
   lock.unlock();
   condition.notify_one();
}

void mysql_db_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t ) {
   try {
      if( !start_block_reached ) {
         if( t->block_num >= start_block_num ) {
            start_block_reached = true;
         }
      }
      if(t->block_num > 0 && start_block_reached){
         queue( transaction_trace_queue, t );
      }
   } catch (fc::exception& e) {
      elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}

void mysql_db_plugin_impl::consume_applied_transactions() {
   std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
   
   try {
      while (true) {
         boost::mutex::scoped_lock lock(mtx_applied_trans);
         while ( transaction_trace_queue.empty() &&
                 !done ) {
            condition.wait(lock);
         }

         // capture for processing
         size_t transaction_trace_size = transaction_trace_queue.size();
         if (transaction_trace_size > 0) {
            transaction_trace_process_queue = move(transaction_trace_queue);
            transaction_trace_queue.clear();
         }

         lock.unlock();

         // warn if queue size greater than 75%
         if( transaction_trace_size > (queue_size * 0.75)) {
            wlog("queue size: ${q}", ("q", transaction_trace_size));
         } else if (done) {
            ilog("draining queue, size: ${q}", ("q", transaction_trace_size));
         }

         // process transactions
         auto start_time = fc::time_point::now();
         auto size = transaction_trace_process_queue.size();
         while (!transaction_trace_process_queue.empty()) {
            const auto& t = transaction_trace_process_queue.front();
            process_applied_transaction(t);
            transaction_trace_process_queue.pop_front();
         }
         auto time = fc::time_point::now() - start_time;
         auto per = size > 0 ? time.count()/size : 0;
         if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
            ilog( "process_applied_transaction, time per: ${p}, size: ${s}, time: ${t}", ("s", size)( "t", time )( "p", per ));

         if( transaction_trace_size == 0 &&
             done ) {
            break;
         }
      }
      ilog("mysql_db_plugin consume thread shutdown gracefully");
   } catch (fc::exception& e) {
      elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while consuming block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while consuming block");
   }
}

void ledger_plugin_impl::consume_query_process() {
   try {
      while (true) {
         boost::mutex::scoped_lock lock(mtx);
         while ( query_queue.empty() && !done ) {
            condition.wait(lock);
         }
         
         size_t query_queue_count = query_queue.size(); 
         std::string query_str = "";
         if (query_queue_count > 0) {
            query_str = query_queue.front(); 
            query_queue.pop_front(); 
         }

         lock.unlock();

         if (query_queue_count > 0) {
            shared_ptr<MysqlConnection> con = m_connection_pool->get_connection();
            assert(con);
            try{
                  con->execute(query_str, true);

                  m_connection_pool->release_connection(*con);
            } catch (...) {
                  m_connection_pool->release_connection(*con);
            }
         }
      
         if( query_queue_count == 0 && done ) {
            break;
         }      
      }

      ilog("ledger_plugin consume query process thread shutdown gracefully");
   } catch (...) {
      elog("Unknown exception while consuming query");
   }

}

void ledger_plugin_impl::process_add_ledger( const chain::action_trace& atrace ) {

   const auto block_number = atrace.block_num;
   const auto action_id = atrace.receipt.global_sequence ; 
   const auto trx_id    = atrace.trx_id;
   const auto block_time = atrace.block_time;
   
   m_ledger_table->add_ledger(action_id, trx_id, block_number, block_time, atrace.receipt.receiver.to_string(), atrace.act);
      
   for( const auto& inline_atrace : atrace.inline_traces ) {
      process_add_ledger( inline_atrace );
   }
}

void ledger_plugin_impl::process_applied_transaction(const chain::transaction_trace_ptr& t) {
   auto start_time = fc::time_point::now();
   const auto block_number = t->block_num;
   if(block_number == 0) return;

   for( const auto& atrace : t->action_traces ) {
      try {      
         if(atrace.act.name == N(transfer) || atrace.act.name == N(create)) {
            process_add_ledger( atrace );
         }
      } catch(...) {
         wlog("add action traces failed.");
      }
   }   

   auto time = fc::time_point::now() - start_time;
   if( time > fc::microseconds(500000) )
      ilog( "process actions, trans_id: ${r}    time: ${t}", ("r",t->id.str())("t", time) );
}

ledger_plugin_impl::ledger_plugin_impl()
{
    static_ledger_plugin_impl = this; 
    ilog("ledger_plugin_impl");
}

ledger_plugin_impl::~ledger_plugin_impl() {
   if (!startup) {
      try {
         m_ledger_table->finalize(); 

         ilog( "shutdown in process please be patient this can take a few minutes" );
         done = true;

         for (size_t i=0; i< consume_query_threads.size(); i++ ) {
            condition.notify_one();
         }

         for (size_t i=0; i< consume_query_threads.size(); i++ ) {
            consume_query_threads[i].join(); 
         }

         for (size_t i=0; i< consume_applied_threads.size(); i++ ) {
            condition.notify_one();
         }

         for (size_t i=0; i< consume_applied_threads.size(); i++ ) {
            consume_applied_trans_threads[i].join(); 
         }

      } catch( std::exception& e ) {
         elog( "Exception on mysql_db_plugin shutdown of consume thread: ${e}", ("e", e.what()));
      }
   }

   static_ledger_plugin_impl = nullptr; 
}

ledger_plugin::ledger_plugin():my(new ledger_plugin_impl()){}
ledger_plugin::~ledger_plugin(){}

void ledger_plugin_impl::wipe_database() {
   ilog("wipe tables");

   // drop tables
   // m_ledger_table->drop();   
   // m_ledger_table->create(); 

   ilog("create tables done");
}

void ledger_plugin_impl::init(const std::string host, const std::string user, const std::string passwd, const std::string database, 
      const uint16_t port, const uint16_t max_conn, bool do_close_on_unlock, uint32_t block_num_start, const variables_map& options) 
{
   m_connection_pool = std::make_shared<connection_pool>(host, user, passwd, database, port, max_conn, do_close_on_unlock);

   {
      uint32_t ledger_raw_ag_count = 10;
      uint32_t ledger_acc_ag_count = 12;
      if( options.count( "ledger-db-ag-raw" )) {
            ledger_raw_ag_count = options.at("ledger-db-ag-raw").as<uint32_t>();
      }
      if( options.count( "ledger-db-ag-acc" )) {
            ledger_acc_ag_count = options.at("ledger-db-ag-acc").as<uint32_t>();
      }
      ilog(" aggregate ledger raw: ${n}", ("n", ledger_raw_ag_count));
      ilog(" aggregate ledger acc: ${n}", ("n", ledger_acc_ag_count));
      m_ledger_table = std::make_unique<ledger_table>(m_connection_pool, ledger_raw_ag_count, ledger_acc_ag_count);
   }
   
   m_block_num_start = block_num_start;
   system_account = chain::name(chain::config::system_account_name).to_string();

   if( wipe_database_on_startup ) {
      wipe_database();
   } 
   // else {
   //    ilog("create tables");
      
   //    // create Tables
   //    m_accounts_table->create();
   //    m_blocks_table->create();
   //    m_transactions_table->create();
   //    m_actions_table->create();

   //    m_accounts_table->create_index();
   //    m_blocks_table->create_index();
   //    m_transactions_table->create_index();
   //    m_actions_table->create_index();
   // }

/*
   // get last action_id from actions table
   if( start_action_idx > 0 ) {
      m_action_id = start_action_idx;
   } else {
      //m_action_id = m_actions_table->get_max_id_raw() + 1;
      m_action_id = m_actions_table->get_max_id() + 1;
   }
*/

   ilog("starting ledger plugin thread");

   for (size_t i=0; i<query_thread_count; i++) {
      consume_query_threads.push_back( boost::thread([this] { consume_query_process(); }) );
      consume_applied_trans_threads.push_back(boost::thread([this] { consume_applied_transactions(); }))
   }

   
   startup = false;
}

void ledger_plugin::set_program_options(options_description&, options_description& cfg) {
   cfg.add_options()
         ("ledger-queue-size", bpo::value<uint32_t>()->default_value(100000),
         "Query queue size.")
         ("ledger-db-query-thread", bpo::value<uint32_t>()->default_value(4),
         "Query work thread count.")
         ("ledger-data-wipe", bpo::bool_switch()->default_value(false),
         "Required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks to wipe ledger table."
         "This option required to prevent accidental wipe of ledger db.")
         ("ledger-db-host", bpo::value<std::string>(),
         "ledger DB host address string")
         ("ledger-db-port", bpo::value<uint16_t>()->default_value(3306),
         "ledger DB port integer")
         ("ledger-db-user", bpo::value<std::string>(),
         "ledger DB user id string")
         ("ledger-db-passwd", bpo::value<std::string>(),
         "ledger DB user password string")
         ("ledger-db-database", bpo::value<std::string>(),
         "ledger DB database name string")
         ("ledger-db-max-connection", bpo::value<uint16_t>()->default_value(5),
         "ledger DB max connection. " 
         "Should be one or more larger then ledger-db-query-thread value.")
         ("ledger-db-block-start", bpo::value<uint32_t>()->default_value(0),
         "If specified then only abi data pushed to ledger db until specified block is reached.")
         ("ledger-db-block-end", bpo::value<uint32_t>()->default_value(0),
         "stop when reached end block number.")
         ("ledger-db-close-on-unlock", bpo::bool_switch()->default_value(false),
         "Close connection from db when release lock.")
         
         // bulk aggregation count
         ("ledger-db-ag-raw", bpo::value<uint32_t>(),
         "ledger raw db aggregation count")
         ("ledger-db-ag-acc", bpo::value<uint32_t>(),
         "ledger acc db aggregation count")
         ;
}

void ledger_plugin::plugin_initialize(const variables_map& options) {
   try {
      if( options.count( "ledger-db-host" )) {
         ilog( "initializing ledger_plugin" );
         my->configured = true;

         if( options.at( "replay-blockchain" ).as<bool>() || options.at( "hard-replay-blockchain" ).as<bool>() || options.at( "delete-all-blocks" ).as<bool>() ) {
            if( options.at( "ledger-data-wipe" ).as<bool>()) {
               ilog( "Wiping mysql database on startup" );
               my->wipe_database_on_startup = true;
            } else if( options.count( "ledger-db-block-start" ) == 0 ) {
               EOS_ASSERT( false, chain::plugin_config_exception, "--ledger-data-wipe required with --replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks"
                                 " --ledger-data-wipe will remove all EOS collections from mysqldb." );
            }
         }

        /*
         if( options.at( "replay-blockchain" ).as<bool>() || options.at( "hard-replay-blockchain" ).as<bool>() ) {
            my->is_replaying = true; 
         }
         */

         if( options.count( "ledger-db-block-end" ) ) {
            my->end_block_num = options.at("ledger-db-block-end").as<uint32_t>();
         }
         if( options.count( "abi-serializer-max-time-ms") == 0 ) {
            EOS_ASSERT(false, chain::plugin_config_exception, "--abi-serializer-max-time-ms required as default value not appropriate for parsing full blocks");
         }
         my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

         if( options.count( "ledger-queue-size" )) {
            my->max_queue_size = options.at( "ledger-queue-size" ).as<uint32_t>();
         }

         if( options.count( "ledger-db-query-thread" )) {
            my->query_thread_count = options.at( "ledger-db-query-thread" ).as<uint32_t>();
         }
         
         if( options.count( "ledger-db-block-start" )) {
            my->start_block_num = options.at( "ledger-db-block-start" ).as<uint32_t>();
         }
         if( options.count( "producer-name") ) {
            wlog( "Ledger plugin not recommended on producer node" );
            //my->is_producer = true;
         }
         /*
         if( options.count( "mysqldb-action-idx")) {
            my->start_action_idx = options.at( "mysqldb-action-idx" ).as<uint64_t>();
         }
         */
         if( my->start_block_num == 0 ) {
            my->start_block_reached = true;
         }

         uint16_t port = 3306;
         uint16_t max_conn = 5;

         // create mysql db connection pool
         std::string host_str = options.at("ledger-db-host").as<std::string>();
         if( options.count( "ledger-db-port" )) {
            port = options.at("ledger-db-port").as<uint16_t>();
         }
         std::string userid = options.at("ledger-db-user").as<std::string>();
         std::string pwd = options.at("ledger-db-passwd").as<std::string>();
         std::string database = options.at("ledger-db-database").as<std::string>();
         if( options.count( "ledger-db-max-connection" )) {
            max_conn = options.at("ledger-db-max-connection").as<uint16_t>();
         }

         
         // hook up to signals on controller
         chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
         EOS_ASSERT( chain_plug, chain::missing_chain_plugin_exception, ""  );
         auto& chain = chain_plug->chain();
         my->chain_id.emplace( chain.get_chain_id());

         my->applied_transaction_connection.emplace(
            chain.applied_transaction.connect( [&]( const chain::transaction_trace_ptr& t ) {
               my->applied_transaction( t );
            } ));
         
         ilog( "connect to ${h}:${p}. ${u}@${d} ", ("h", host_str)("p", port)("u", userid)("d", database));
         bool close_on_unlock = options.at("ledger-db-close-on-unlock").as<bool>();
         my->init(host_str, userid, pwd, database, port, max_conn, close_on_unlock, my->start_block_num, options);
         
      } else {
         wlog( "eosio::ledger_plugin configured, but no --ledger-db-uri specified." );
         wlog( "ledger_plugin disabled." );
      }
   } FC_LOG_AND_RETHROW()
}

void ledger_plugin::plugin_startup() {
   // Make the magic happen
}

void ledger_plugin::plugin_shutdown() {
   // OK, that's enough magic
   my->applied_transaction_connection.reset();
   my.reset();
}

void post_query_str_to_queue(const std::string query_str) {
      if (!static_ledger_plugin_impl) return; 

      static_ledger_plugin_impl->queue(
            static_ledger_plugin_impl->query_queue, query_str
      );
}

}
