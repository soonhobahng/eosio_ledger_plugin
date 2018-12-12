/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#pragma once
#include <eosio/chain_plugin/chain_plugin.hpp>
#include <appbase/application.hpp>
#include <memory>

namespace eosio {

using ledger_plugin_impl_ptr = std::shared_ptr<class ledger_plugin_impl>;

class ledger_plugin : public plugin<ledger_plugin> {
public:
   APPBASE_PLUGIN_REQUIRES((chain_plugin))
   
   ledger_plugin();
   virtual ~ledger_plugin();

   virtual void set_program_options(options_description&, options_description& cfg) override;
 
   void plugin_initialize(const variables_map& options);
   void plugin_startup();
   void plugin_shutdown();

private:
   ledger_plugin_impl_ptr my;
};

}
