-module(erlcql_cluster_server).

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([new/2,
         checkout/1,
         checkin/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
          dispatch_table    :: ets:tid()
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

new(Name, Opts) ->
    gen_server:call(?MODULE, {new, Name, Opts}).

checkout(Name) ->
    random:seed(now()),
    {_, NodePools} = pool_find(Name),
    NodePool = lists:nth(random:uniform(length(NodePools)), NodePools),
    Worker = poolboy:checkout(NodePool),
    {ok, {NodePool, Worker}}.

checkin({NodePool, Worker}) ->
    poolboy:checkin(NodePool, Worker).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    DTid = ets:new(?MODULE, [named_table, public, {read_concurrency, true}]),
    {ok, #state{dispatch_table=DTid}}.

handle_call({new, Name, Options}, _From, #state{}=State) ->
    case already_exists(Name) of
        true ->
            {reply, {error, already_started}, State};
        false ->
            true = pool_new(Name, Options),
            {reply, ok, State#state{}}
    end.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
already_exists(Name) ->
    case ets:lookup(?MODULE, Name) of
        [] -> false;
        _  -> true
    end.

pool_find(Name) ->
    [PoolInfo] = ets:lookup(?MODULE, Name),
    PoolInfo.

pool_new(Name, Options) ->
    Nodes = proplists:get_value(nodes, Options),
    ets:insert(?MODULE, {Name, pool_new_per_node(Name, Nodes, Options)}).

pool_new_per_node(Name, Nodes, Options) ->
    pool_new_per_node(Name, Nodes, Options, []).

pool_new_per_node(_Name, [], _Options, Pools) ->
    Pools;
pool_new_per_node(Name, [{Host, Port}=Node|Nodes], Options, Pools) ->
    PoolName = pool_name(Name, Node),
    PoolOptions = pool_options(Options),
    ErlCqlOptions = erlcql_options(Host, Port, Options),
    {ok, _} = erlcql_poolboy:start_link(PoolName, PoolOptions, ErlCqlOptions),
    pool_new_per_node(Name, Nodes, Options, [PoolName|Pools]).

pool_name(Name, {Host, Port}) ->
    list_to_atom( atom_to_list(Name) ++ Host ++ integer_to_list(Port) ).

pool_options(Options) ->
    Size     = proplists:get_value(pool_size, Options),
    Overflow = proplists:get_value(pool_overflow, Options),
    [{size, Size}, {max_overflow, Overflow},
     {worker_module, erlcql_cluster_worker}].

erlcql_options(Host, Port, Options) ->
    Keyspace   = proplists:get_value(use, Options),
    Statements = proplists:get_value(prepare, Options),
    [{host, Host},
     {port, Port},
     {use, Keyspace},
     {keepalive, true},
     {cql_version, <<"3.1.1">>},
     {auto_reconnect, true},
     {prepare, Statements}].
