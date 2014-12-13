-module(erlcql_cluster_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Mod), {Mod, {Mod, start_link, []},
                     permanent, 5000, worker, [Mod]}).

%%%===================================================================
%%% API functions
%%%===================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================
init([]) ->
    {ok, {{one_for_one, 5, 10}, [?CHILD(erlcql_cluster_server)]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
