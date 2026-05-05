# newCache 升级实现补充说明：后三环与迁移详解

这是一份**补充文档**，不是对现有源码的修改说明，也不会覆盖 `docs/newcache-upgrade-implementation-details.md`。

本文重点补三件事：

1. 后面 3 个环到底怎么落到代码里
2. 第 5 层为什么必须补齐到协议级别
3. 数据迁移到底怎么做，为什么要这么做

本文默认你已经读过：

- `docs/newcache-upgrade-guide.md`
- `docs/newcache-upgrade-implementation-details.md`

如果两份文档里有局部命名不一致，以本文的“目标实现”说明为准。

---

## 一、先把问题说清楚

当前这套 `newCache` 的核心能力已经有了：

- `cache.Group`
- `internal/client/picker.go`
- `internal/consistenthash`
- `etcd` 服务发现
- gRPC 节点通信

但真正缺的是三个闭环：

1. **写入协议闭环**：Set / Delete / 批量迁移没有统一语义
2. **节点角色闭环**：warming / active / draining 还没完全变成路由规则
3. **数据位置闭环**：哈希环变化后，数据本身没有自动搬迁

所以后三环不是“再加几个 ring 对象”这么简单，而是要把：

- 节点状态
- 写入 owner
- 影子双写
- 迁移扫描
- 批量落盘
- tombstone 防复活

串成一条完整链路。

---

## 二、第 5 层：先把协议补齐

这一层的目标不是“写个 RPC 方法”，而是让远程节点也能表达完整语义：

- 这是普通写，还是迁移写
- 这是新版本，还是旧版本
- 这是普通 value，还是 tombstone
- TTL 还剩多少
- 这条数据是本地发起，还是 peer 转发

如果第 5 层不补齐，后面的三环和迁移都会被迫退化成“只会搬 bytes”，最后会丢版本、丢 TTL、丢删除语义。

### 2.1 为什么必须扩协议

只传 `key + value` 不够，原因很直接：

1. **版本不能丢**
   - 迁移时可能同时存在旧节点和新节点
   - 旧数据晚到一步，就可能覆盖新数据
2. **TTL 不能丢**
   - 不然原本 30 秒后过期的值，迁移后变成永久值
3. **Delete 不能只做物理删除**
   - 分布式里“删掉”不代表历史旧值不会回来
   - 需要 tombstone + version + 保护 TTL
4. **peer 转发不能回环**
   - 没有 `from_peer`，节点可能把请求继续转发回去，形成环形调用

### 2.2 第 5 层应该包含什么

建议协议层至少包含：

- `Get`
- `Set`
- `Delete`
- `Scan`
- `BatchSet`

其中：

- `Get` 负责读
- `Set` 负责普通写和迁移写
- `Delete` 负责 tombstone
- `Scan` 负责把本地数据扫出来
- `BatchSet` 负责把一批迁移结果写进去

### 2.3 推荐消息字段

如果按“可迁移、可回放、可去重”的要求设计，消息里至少要有这些字段：

```proto
message Entry {
  string group = 1;
  string key = 2;
  bytes value = 3;
  int64 ttl_ms = 4;
  int64 version = 5;
  bool tombstone = 6;
}
```

字段含义：

- `group`：目标缓存组
- `key`：缓存键
- `value`：值本身
- `ttl_ms`：剩余 TTL，不是原始 TTL
- `version`：版本号
- `tombstone`：是否为删除墓碑

### 2.4 为什么 `ttl_ms` 要传“剩余 TTL”

因为迁移时，数据已经在旧节点上活了一段时间。

举个例子：

- 原始 TTL 是 10 分钟
- 旧节点已经存了 8 分钟
- 此时迁移到新节点

如果你把“原始 10 分钟”重新写过去，新节点就会多活 8 分钟，这会破坏一致性。

所以迁移传输的应该是：

```text
remaining_ttl = expire_at - now
```

这也是为什么 `Scan` 返回时不能只返回 `value`。

### 2.5 为什么 `Delete` 要带 version

因为 Delete 本质上也是一条写入。

如果不带 version，就会出现这种情况：

```text
version=10: SET user:1 = A
version=11: DELETE user:1
version=9 : 延迟到达的旧 SET user:1 = A
```

如果 Delete 只是简单删掉 key，那么版本 9 的旧写会把 key 复活。

所以正确做法是：

- Delete 写 tombstone
- tombstone 也带 version
- tombstone 也有 TTL

### 2.6 第 5 层和现有代码的对应关系

你现在仓库里已经有这些相关文件：

- `cache/peers.go`
- `internal/client/client.go`
- `internal/server/server.go`
- `api/proto/cache.proto`

对应关系应该是：

- `cache.PeerGetter` 只保留“远程怎么调用”这个接口
- `client.Client` 负责把 Go 方法转成 gRPC 请求
- `server.Server` 负责把 gRPC 请求转回 `cache.Group`
- `cache.proto` 负责把字段定义固定下来

也就是说，第 5 层不是“多写几个方法”，而是把**分布式语义**固定在协议里。

---

## 三、三个环到底是什么

这里的三个环，不是三个重复哈希表，而是三个**角色不同**的环：

```text
readRing   -> 读路由
writeRing  -> 写路由
shadowRing -> 迁移双写
```

### 3.1 为什么不能只保留一个环

一个环只能告诉你“这个 key 归谁”，但不能回答下面这些问题：

- 新节点刚加入时，能不能读？
- 新节点刚加入时，能不能写？
- 迁移期间，旧节点写入要不要同步到未来 owner？
- 正在下线的节点，是否还允许承接读请求？

这些问题不是哈希函数能解决的，必须由“节点状态 + 多环策略”解决。

### 3.2 三个环的职责

#### 读环 `readRing`

职责：

- 给 `Get` 用
- 允许 `active` 和 `draining`
- 不包含 `warming`

原因：

- `warming` 节点还没迁完数据，不适合接读
- `draining` 节点准备下线，但它手里可能还有旧数据，适合兜底读

#### 写环 `writeRing`

职责：

- 给 `Set` / `Delete` 用
- 只包含 `active`

原因：

- `warming` 不能接主写
- `draining` 不能接新写
- 只有 `active` 才是当前 owner

#### 影子环 `shadowRing`

职责：

- 给迁移期间的双写用
- 包含 `active + warming`

原因：

- `warming` 节点虽然不能接主读主写，但它是“未来 owner”
- 为了保证迁移过程中新增写不会丢，必须把变更同步给它

### 3.3 节点状态和环的关系

建议节点状态固定成三种：

```text
warming   -> 新节点，正在补数据
active    -> 正常服务
draining  -> 准备下线，停止新写
```

状态到环的映射：

```text
warming   -> shadowRing
active    -> readRing + writeRing + shadowRing
draining  -> readRing
```

### 3.4 为什么 `draining` 还要留在读环

因为下线不是瞬间完成的。

如果一个节点刚切到 `draining`，但新 owner 还没完全接管数据，那么旧节点还需要继续回答一部分读请求，避免出现短时间 miss。

所以：

- 写要停
- 读先别停
- 等迁移完成再完全退出

### 3.5 为什么 `warming` 不能进读环

因为 `warming` 节点的数据是不完整的。

如果它提前接读，读到的可能是：

- 部分迁移数据
- 旧版本数据
- 甚至空值

所以 `warming` 的唯一职责是：

- 接收迁移写
- 接收影子双写
- 不对外承接主读

---

## 四、三环的代码应该怎么接

这一部分讲“怎么做”，不是只讲概念。

### 4.1 Picker 里要保存什么

`internal/client/picker.go` 里应该保存：

```go
type Picker struct {
    mu         sync.RWMutex
    selfAddr   string
    readRing   *consistenthash.Map
    writeRing  *consistenthash.Map
    shadowRing *consistenthash.Map
    clients    map[string]*Client
    nodes      map[string]registry.NodeInfo
}
```

核心点：

- `readRing`、`writeRing`、`shadowRing` 不能互相替代
- `clients` 是地址到 gRPC client 的映射
- `nodes` 是节点元信息，不只是地址

### 4.2 reload 时要读 NodeInfo

etcd 里不能只存 `addr`，要存 JSON 形式的 `NodeInfo`：

```go
type NodeInfo struct {
    Addr   string
    Status NodeStatus
}
```

reload 的时候要先反序列化，再按状态建环。

如果只存字符串地址，就没法区分：

- 这是 active 还是 warming
- 这是该参与读环还是写环

### 4.3 buildRings 的规则

构造三个环时，逻辑应该是：

```go
switch n.Status {
case StatusActive:
    readRing.Add(n.Addr)
    writeRing.Add(n.Addr)
    shadowRing.Add(n.Addr)
case StatusWarming:
    shadowRing.Add(n.Addr)
case StatusDraining:
    readRing.Add(n.Addr)
}
```

### 4.4 三个选择器分别干什么

建议 Picker 提供三个方法：

```go
PickReadPeer(key string)
PickWritePeer(key string)
PickShadowPeer(key string)
```

语义：

- `PickReadPeer`：给 Get 用
- `PickWritePeer`：给 Set/Delete 用
- `PickShadowPeer`：给迁移双写用

### 4.5 为什么还要保留旧的 `PickPeer`

因为可以过渡。

旧逻辑里很多地方默认都调用 `PickPeer`，如果一下全改掉，改动范围会很大。

所以过渡期可以让：

```go
PickPeer(key) == PickReadPeer(key)
```

然后逐步把写路径切换到 `PickWritePeer`。

### 4.6 路由层的真实决策

#### Get 路由

```text
Get(key)
  -> PickReadPeer(key)
  -> 如果是自己，走本地
  -> 如果是远程，走 gRPC Get
```

#### Set 路由

```text
Set(key)
  -> PickWritePeer(key)
  -> 如果 owner 是远程，转发给 owner
  -> 如果 owner 是自己，写本地
```

#### 迁移双写路由

```text
迁移中的写入
  -> 写到当前 owner
  -> 同时写到 shadow owner
```

---

## 五、数据迁移为什么必须做

一致性哈希只解决一件事：

```text
给定 key，应该由谁负责
```

它**不负责搬数据**。

所以一旦节点变化，就会出现这个问题：

```text
环变了
owner 变了
但数据还在旧节点
```

如果不迁移，就会 miss。

### 5.1 扩容时会发生什么

新节点加入后，哈希环重新分布，一部分 key 的 owner 会变成新节点。

但这些 key 的 value 还在旧节点。

于是：

- 路由已经把请求发给新节点
- 新节点却没有数据
- 结果就是 miss

### 5.2 缩容时会发生什么

旧节点退出时，它手里还握着一批数据。

如果直接下线：

- 这些 key 就丢了
- 即使新环已经重建，也找不到旧 value

所以缩容前必须把数据先推走。

---

## 六、数据迁移的核心原则

迁移不能只搬 `value`，还必须搬这些信息：

1. `version`
2. `ttl`
3. `tombstone`
4. `expireAt`

### 6.1 为什么版本必须搬

版本是“谁更新得更晚”的判断依据。

没有版本，旧数据和新数据就没法比较，迁移后很容易覆盖新值。

### 6.2 为什么 tombstone 必须搬

Delete 不是“真删”，而是“打删除标记”。

如果 tombstone 不迁移，旧 key 可能在迁移后复活。

### 6.3 为什么 TTL 必须搬

如果一个 key 还有 20 秒过期，迁移后变成永久值，就破坏了缓存语义。

### 6.4 为什么要传剩余 TTL

因为迁移发生在运行中，不是在 key 刚创建的时候。

所以：

```text
remaining_ttl = expireAt - now
```

而不是重新用原始 TTL 计算。

---

## 七、迁移的数据模型

建议迁移层不要直接搬 `ByteView`，而是搬一个完整记录：

```go
type CacheEntryRecord struct {
    Group     string
    Key       string
    Value     []byte
    TTL       time.Duration
    Version   int64
    Tombstone bool
}
```

如果需要更精确，也可以额外带：

```go
ExpireAt time.Time
```

但从迁移协议角度看，`TTL` 已经足够表达剩余生命周期。

### 7.1 记录里为什么要带 Group

因为迁移可能跨组复用同一套 RPC。

加上 `group` 可以让 `BatchSet` 不只服务单一缓存空间。

### 7.2 记录里为什么要带 Tombstone

因为删除也要迁移。

否则迁移后的新 owner 只会拿到 value，不会知道这个 key 已经被删过。

---

## 八、迁移分成两条路径

### 8.1 扩容迁移：Pull

新节点刚上线，自己去拉属于自己的数据。

流程：

1. 新节点先注册为 `warming`
2. 构造 future ring
3. 找出应该归自己负责的 key
4. 从旧节点 `Scan`
5. 把记录 `BatchSet` 到自己
6. 完成后切到 `active`

为什么叫 Pull：

- 新节点主动去拉
- 旧节点不需要预先知道全部迁移目标

### 8.2 缩容迁移：Push

旧节点准备退出，主动把本地数据推给新 owner。

流程：

1. 节点切为 `draining`
2. 停止接新写
3. 扫描本地数据
4. 根据新写环找新 owner
5. 推送到新 owner
6. 推完后从 etcd 注销

为什么叫 Push：

- 数据是旧节点主动推送
- 退出方最清楚自己还有哪些 key

---

## 九、迁移接口怎么设计

### 9.1 Scan

`Scan` 的作用是把本地数据分页导出。

建议参数：

```proto
message ScanRequest {
  string group = 1;
  string start_key = 2;
  int64 count = 3;
}
```

返回：

```proto
message ScanResponse {
  repeated Entry entries = 1;
}
```

### 9.2 BatchSet

`BatchSet` 的作用是一次写入一批迁移记录。

建议参数：

```proto
message BatchSetRequest {
  repeated Entry entries = 1;
}
```

返回：

```proto
message BatchSetResponse {
  bool ok = 1;
}
```

### 9.3 为什么要批量

因为迁移不是单点写，而是成百上千个 key。

如果每个 key 都单独 RPC：

- 网络开销大
- 性能差
- 重试成本高

批量写可以显著降低开销。

---

## 十、迁移时怎么保证不出错

这是最关键的部分。

### 10.1 只写更“新”的版本

迁移写入前，要和目标节点现有版本比较：

```text
incoming.version < current.version
    -> 丢弃
incoming.version >= current.version
    -> 接受
```

这样可以防止旧数据覆盖新数据。

### 10.2 tombstone 也要参与版本比较

Delete 不是特殊通道，它也是一条版本化写入。

如果目标节点已经有更高版本的 tombstone，就不能再被旧 value 覆盖。

### 10.3 TTL 过期的数据不要迁

如果扫描出来的记录已经过期，就没有必要迁移了。

迁移前应该先判断：

```text
if ttl <= 0:
    skip
```

### 10.4 迁移应该幂等

迁移过程中，网络重试很常见。

所以 `BatchSet` 必须天然支持重复调用：

- 同一条记录重复写，不应该出错
- 同一条记录重复写，不应该覆盖更高版本

### 10.5 为什么需要“扫描 + 去重 + 版本判断”

因为迁移不是一次原子操作。

现实里可能发生：

- 扫描时有并发写
- 批量写时某个节点超时
- 重试又来了第二次

所以迁移逻辑必须能抵抗重复和乱序。

---

## 十一、迁移的具体步骤

这一节给出可以直接落地的顺序。

### 11.1 扩容 Pull 的步骤

#### 第 1 步：新节点注册为 warming

目的：

- 让路由知道这个节点还没准备好
- 让 shadowRing 可以提前包含它

#### 第 2 步：构造 future ring

future ring 的意思是：

```text
如果把 warming 节点也算进去，key 会落到哪里
```

这样才能知道哪些 key 最终应该属于新节点。

#### 第 3 步：从旧节点扫描

扫描旧节点本地缓存，得到：

- key
- value
- remaining ttl
- version
- tombstone

#### 第 4 步：按 ownership 过滤

不是扫描到什么都迁，而是只迁未来 owner 应该持有的 key。

#### 第 5 步：BatchSet 到新节点

目标节点收到批量记录后，逐条版本比较并落入缓存。

#### 第 6 步：完成后切 active

一旦新节点数据齐了，就把状态从 `warming` 切成 `active`。

### 11.2 缩容 Push 的步骤

#### 第 1 步：节点切 draining

先停止接新写。

#### 第 2 步：构造新写环

把准备下线的节点从 writeRing 里去掉。

#### 第 3 步：扫描本地数据

把本地 key 全部扫出来。

#### 第 4 步：计算每个 key 的新 owner

用新的 writeRing 重新定位目标节点。

#### 第 5 步：推送到新 owner

把完整记录批量写过去。

#### 第 6 步：注销节点

所有需要的数据推完以后，再从 etcd 里删除服务信息。

---

## 十二、为什么迁移一定要和三环配合

单独做迁移不够，必须和三环一起做。

### 12.1 没有三环的问题

如果只有一个环：

- warming 节点可能提前接读
- draining 节点可能还在接写
- 迁移期间无法区分主写和影子写

### 12.2 没有迁移的问题

如果只有三环，没有迁移：

- 路由是对的
- 数据位置是错的

结果还是 miss。

### 12.3 正确顺序

正确的顺序是：

```text
节点状态变化
  -> 三环重建
  -> 迁移任务启动
  -> shadow 双写保证增量
  -> 数据齐备
  -> 切 active / 完成下线
```

---

## 十三、建议的实现分层

### 13.1 协议层

负责：

- Get / Set / Delete / Scan / BatchSet
- version / ttl / tombstone

文件：

- `api/proto/cache.proto`

### 13.2 客户端层

负责：

- 把 Go 方法转成 gRPC 请求
- 控制超时
- 处理 from_peer

文件：

- `internal/client/client.go`

### 13.3 服务层

负责：

- 接收 gRPC
- 转给 `cache.Group`
- 做参数校验

文件：

- `internal/server/server.go`

### 13.4 路由层

负责：

- 根据节点状态建三个环
- 选择 read/write/shadow 目标

文件：

- `internal/client/picker.go`

### 13.5 业务层

负责：

- 版本比较
- tombstone 写入
- 迁移回填
- 本地缓存一致性

文件：

- `cache/group.go`
- `cache/cache.go`
- `cache/cache_entry.go`

---

## 十四、和现有代码的关系

下面这几处是你现在最关键的承接点：

### 14.1 `cache/group.go`

这里是核心业务入口。

应该承接：

- `Set`
- `Delete`
- `Get`
- `populateEntry`
- `BatchSet`
- `Scan`

### 14.2 `internal/client/picker.go`

这里决定三环怎么建。

应该承接：

- `reload`
- `buildRings`
- `PickReadPeer`
- `PickWritePeer`
- `PickShadowPeer`

### 14.3 `internal/server/server.go`

这里把 gRPC 请求翻译成缓存操作。

应该承接：

- `Get`
- `Set`
- `Delete`
- `Scan`
- `BatchSet`

### 14.4 `internal/client/client.go`

这里负责远程调用。

应该承接：

- 超时控制
- `FromPeer`
- `SetWithMeta`
- `DeleteWithVersion`
- `Scan`
- `BatchSet`

---

## 十五、你实现时最容易踩的坑

### 15.1 只迁移 value，不迁 version

后果：

- 旧值覆盖新值
- Delete 失效

### 15.2 只迁移 value，不迁 tombstone

后果：

- 已删除 key 被复活

### 15.3 只迁移原始 TTL

后果：

- key 活得比预期更久

### 15.4 warming 节点提前进读环

后果：

- 读到半成品数据

### 15.5 draining 节点继续接新写

后果：

- 下线期间数据分裂

### 15.6 peer 请求不打标识

后果：

- 请求被不断转发，形成循环

### 15.7 迁移不是幂等

后果：

- 重试导致脏写

---

## 十六、推荐的最小实现顺序

如果你想把后 3 个环和迁移真正做出来，建议按这个顺序：

### 第一步：补协议

先让 `Set/Delete/Scan/BatchSet` 把版本、TTL、tombstone 讲清楚。

### 第二步：补三环

先把读写和 shadow 路由明确下来。

### 第三步：补迁移记录

定义完整的迁移 entry，不要只传 value。

### 第四步：做 Pull

先实现扩容迁移，逻辑更直观。

### 第五步：做 Push

再实现缩容迁移。

### 第六步：补测试

重点测：

- 旧版本不覆盖新版本
- tombstone 不复活
- warming 不接读
- draining 不接写
- 迁移后 key 还能命中

---

## 十七、结论

后三环不是单独功能，它们解决的是同一件事：

```text
节点变了，数据还要对，读写还要稳。
```

所以正确的落地方式不是先谈性能，而是先把语义补齐：

1. 协议层支持 version / ttl / tombstone
2. 路由层支持 read / write / shadow 三环
3. 迁移层支持 Scan / BatchSet / Pull / Push
4. 状态层支持 warming / active / draining

只要这四层闭环，扩缩容就不再是“换个环”，而是一次可控的数据重定位。

