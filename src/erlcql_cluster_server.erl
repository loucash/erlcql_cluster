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

-record(pool, {
          name      :: atom(),
          nodes     :: [atom()]
         }).

-record(state, {
          pools = []        :: [#pool{}]
         }).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

new(Name, Opts) ->
    gen_server:call(?MODULE, {new, Name, Opts}).

checkout(Name) ->
    gen_server:call(?MODULE, {checkout, Name}).

checkin(Resource) ->
    gen_server:call(?MODULE, {checkin, Resource}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    random:seed(now()),
    {ok, #state{}}.

handle_call({checkin, {NodePool, Worker}}, _From, State) ->
    Reply = poolboy:checkin(NodePool, Worker),
    {reply, Reply, State};
handle_call({checkout, Name}, _From, #state{pools=Pools}=State) ->
    #pool{nodes=NodePools} = lists:keyfind(Name, #pool.name, Pools),
    NodePool = lists:nth(random:uniform(length(NodePools)), NodePools),
    Worker = poolboy:checkout(NodePool),
    {reply, {ok, {NodePool, Worker}}, State};
handle_call({new, Name, Options}, _From, #state{pools=Pools}=State) ->
    case already_exists(Name, Pools) of
        true ->
            {reply, {error, already_started}, State};
        false ->
            {reply, ok, State#state{pools=pool_new(Name, Options, Pools)}}
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
already_exists(Name, Pools) ->
    lists:keymember(Name, #pool.name, Pools).

pool_new(Name, Options, Pools) ->
    NewPool = #pool{name=Name},
    Nodes = proplists:get_value(nodes, Options),
    [NewPool#pool{nodes=pool_new_per_node(Name, Nodes, Options)}|Pools].

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
