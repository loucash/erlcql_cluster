-module(erlcql_cluster).
-export([start/0, stop/0]).
-export([new/2,
         checkout/1,
         checkin/1]).
-export([get_env/1, get_env/2]).

-define(APP, ?MODULE).

start() ->
    application:ensure_all_started(?APP).

stop() ->
    application:stop(?APP).

new(Name, Opts) ->
    erlcql_cluster_server:new(Name, Opts).

checkout(Name) ->
    erlcql_cluster_server:checkout(Name).

checkin(Resource) ->
    erlcql_cluster_server:checkin(Resource).

get_env(Name) ->
    application:get_env(?APP, Name).

get_env(Name, Default) ->
    application:get_env(?APP, Name, Default).
