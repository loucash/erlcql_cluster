erlcql_cluster
==============

Pool of pools of erlcql connections to separate cassandra cluster nodes

## How to build it

`make deps compile`

## Tutorial

Start an application:

```erlang
1> erlcql_cluster:start().
{ok,[snappy,lz4,erlcql,poolboy,erlcql_poolboy,
     erlcql_cluster]}
```

Create new pool `test` for cluster with nodes.

```erlang
2> erlcql_cluster:new(test, [{nodes, [{"localhost", 9160}]},
                                      {pool_size, 1},
                                      {pool_overflow, 1},
                                      {use, "kairosdb"},
                                      {prepare, []}]).
ok
```

Checkout a resource and get erlcql client:

```erlang
3> {ok, {Pool, Worker}} = erlcql_cluster:checkout(test).
{ok,{testlocalhost9160,<0.59.0>}}

4> erlcql_cluster_worker:get_client(Worker).
<0.60.0>
```

Checkin resource:

```erlang
5> erlcql_cluster:checkin({Pool, Worker}).
ok
```
