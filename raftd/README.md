## raftd
> 源码链接：[https://github.com/goraft/raftd](https://github.com/goraft/raftd)

这里简单通过 DB 表示 key-value 数据库对象，相关 HTTP API 如下：

```shell
# Set the value of a key.
$ curl -X POST http://localhost:4001/db/my_key -d 'FOO'
```

```shell
# Retrieve the value for a given key.
$ curl http://localhost:4001/db/my_key
FOO
```

发送给 leader 的所有值将被传播到集群中的其他服务器。这里不支持命令转发。如果你试图将更改发送给一个follower，那么它将被简单地拒绝。


## 启动
```shell
Usage: ./raftd [arguments] <data-path> 
  -debug
        Raft debugging
  -h string
        hostname (default "localhost")
  -join string
        host:port of leader to join
  -p int
        port (default 4001)
  -trace
        Raft trace debugging
  -v    verbose logging
Data path argument required
```

启动节点1：
```shell
$ raftd -p 4001 ./testpath/p1
```

启动节点2、3（当你重新启动节点时，它已经加入到集群中，因此您可以删除-join参数。）：
```shell
$ raftd -p 4001 -join localhost:4002 ./testpath/p2
$ raftd -p 4001 -join localhost:4002 ./testpath/p3
```

为 leader 设置值：
```shell
$ curl -XPOST localhost:4001/db/foo -d 'bar'
```

这些值将被传播给 follower:
```shell
$ curl localhost:4001/db/foo
bar
$ curl localhost:4002/db/foo
bar
$ curl localhost:4003/db/foo
bar
```

杀死 leader 会自动选出新的 leader。如果你杀死并重新启动第一个节点，并尝试设置一个值，你会收到:
```shell
$ curl -XPOST localhost:4001/db/foo -d 'bar'
raft.Server: Not current leader
```

## 说明
运行2节点分布式共识协议的一个问题是，我们需要两台服务器都能正常运行，以建立仲裁并在服务器上执行操作。因此，如果我们此时杀死一台服务器，我们将无法更新系统(因为我们无法复制到大多数)。您需要添加额外的节点，以使故障不影响系统。例如，对于3个节点，可以有1个节点故障。如果有5个节点，可能会有2个节点故障。