-module(erlcql_cluster_worker).

-behaviour(gen_server).

-export([start_link/1,
         get_client/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {client :: pid()}).

-define(KEEPALIVE, 55000).

start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

get_client(Worker) ->
    gen_server:call(Worker, get_client).

init(Opts) ->
    {ok, Pid} = erlcql_client:start_link(Opts),
    {ok, #state{client = Pid}, ?KEEPALIVE}.

handle_call(get_client, _From, #state{client = Pid} = State) ->
    {reply, Pid, State, ?KEEPALIVE};
handle_call(Request, _From, State) ->
    {stop, {bad_call, Request}, State}.

handle_cast(Request, State) ->
    {stop, {bad_cast, Request}, State}.

handle_info(timeout, #state{client = Pid} = State) ->
    %% There's no ping, so send the options request
    _ = case erlcql_client:options(Pid) of
            {ok, _} = Ok -> Ok;
            {error, _} = Error ->
                % warning, not connected
                Error
        end,
    {noreply, State, ?KEEPALIVE};
handle_info(Info, State) ->
    {stop, {bad_info, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
