# 用户态协作式并发运行时（User-Space Fiber Runtime）

一套运行在**当前服务进程内**的用户态协作式并发运行时。执行单元（fiber）由少量工作线程
多路复用调度；挂起时**不占用任何工作线程**；恢复后局部状态完整。运行时不使用操作系统级
抢占，单元内异常会被捕获并转为可查询状态，不会拖垮调用方线程或服务进程。

纯 JDK 17 实现，无第三方运行时依赖。

---

## 1. 设计概览

| 维度 | 方案 |
| --- | --- |
| 并发模型 | 用户态协作式（cooperative），无 OS 抢占 |
| 线程模型 | N 个 daemon 工作线程多路复用全部 fiber |
| 挂起实现 | 堆上续体（CPS 状态机）+ 控制流信号 `SuspendSignal` 展开工作线程栈 |
| 局部状态 | 堆上的 `FiberLocals` + `stage()` 程序计数器，挂起/恢复不丢失 |
| 调度 | 优先级 + 等待时长老化（防饿死）+ 全局序号破平（可复现） |
| 定时 | 逻辑时钟 `Clock` 驱动的最小堆定时轮；支持 `ManualClock` 确定性回放 |
| 结构化 | 父单元必须等待全部子单元；取消父单元级联取消全部后代；无孤儿 |
| 隔离 | 正常完成 / 异常失败 / 被取消三态分明，异常绝不逃逸到调度器或调用方 |

> **为什么是 CPS 状态机而不是"真续体"？**
> 移动 JVM 调用栈需要内部 API（`jdk.internal.vm.Continuation`，虚拟线程同源）。该 API 在部分
> JDK 发行版（如本机默认的 Zulu 17）中被裁剪，依赖它会让默认构建失败。本运行时采用与
> Kotlin 协程在 JVM 上相同的、完全可移植的做法：把"续体"以**对象字段（stage + locals）**
> 的形式保存在堆上。挂起点释放工作线程，恢复时重新进入任务体并跳到 `stage()` 指向的位置。

---

## 2. 快速开始

```java
ManualClock clock = new ManualClock(0);                 // 测试可换成 Clock.system()
FiberSchedulerConfig cfg = FiberSchedulerConfig.builder()
        .clock(clock).workerThreads(1).build();

try (FiberScheduler scheduler = FiberScheduler.create(cfg);
     FiberScope scope = FiberScope.open(scheduler)) {

    Fiber<String> fiber = scope.spawn(ctx -> {
        while (true) {
            switch ((int) ctx.stage()) {
                case 0 -> {
                    ctx.locals().put("k", "v");
                    ctx.stage(1);
                    ctx.sleep(Duration.ofMillis(100));   // 挂起，让出工作线程
                }
                case 1 -> {
                    return "done:" + ctx.locals().get("k"); // 恢复后 locals 完整
                }
                default -> throw new IllegalStateException();
            }
        }
    });

    clock.advance(100);                                  // 推进逻辑时钟触发定时
    String result = fiber.await();                       // "done:v"
}
```

### 任务体的状态机约定（重要）

任务体是一个会被**多次重入**的函数：每次从挂起点恢复时，运行时都会**从头**再次调用它。
因此：

- 每个挂起调用（`park/sleep/yield/join/joinAll/call`）**之前**先 `ctx.stage(n)` 推进程序计数器；
- 用 `switch(ctx.stage())` 把挂起点之后的逻辑放到对应分支，恢复时直接跳过已完成的挂起；
- 需要跨挂起保留的局部变量放进 `ctx.locals()`（或任务对象字段），**不要依赖 JVM 栈局部变量**。

这是可移植 CPS 的固有写法（等价于协程编译器生成的状态机）。若挂起后未推进 stage，
任务体会从头再次执行同一个挂起点，表现为"一直挂起"。

---

## 3. 生命周期与三态结果

`FiberState`：`NEW → READY → RUNNING → （SUSPENDED | TIMED_WAITING | WAITING）→ 终态`。

终态严格区分：

- `COMPLETED`：任务体正常返回，`result()` 可取结果；
- `FAILED`：任务体抛出异常，异常被捕获存于 `failure()`，可查询，不扩散；
- `CANCELLED`：被取消（手动 / 超时 / 父级 / 关闭），`cancelReason()` 给出原因，
  `failure()` 是一个 `FiberCancellationException`。

`fiber.await()` 在调用方线程阻塞到终态，并按结果**正常返回值**或**重抛对应异常**；
重抛只影响该次调用，不影响调度器与其它单元。

---

## 4. 调度语义（优先级、公平性、可复现）

就绪队列为优先级堆：

```
有效权重 = 静态优先级权重(LOW=0 / NORMAL=5 / HIGH=10)
         + min(老化上限, 已等待毫秒 / agingStepMillis)
```

- **高优先级先跑**；同有效权重按**全局创建序号 seq** 先进先出。
- **防饿死**：等待越久老化加成越高，最终可达最高档。高频高优先级提交无法让一个低优先级
  单元长期得不到调度（见 `ReadyQueueAgingTest`）。
- **确定性回放**：`workerThreads(1)` + 固定（手动）时钟下，相同提交输入产生完全相同的
  调度顺序（见 `SchedulingDeterminismTest`，同输入两次结果逐位一致）。

工作线程在没有就绪单元时等待：逻辑时钟下由 `ManualClock.advance(...)` 的变更监听即时唤醒；
墙钟下按最近截止时间有界等待。

---

## 5. 挂起 / 恢复 / 等待操作

| API | 语义 |
| --- | --- |
| `yield()` | 主动让出，回到就绪队列尾部重新参与优先级调度 |
| `park()` | 挂起到外部 `fiber.resume()` 或取消 |
| `park(Duration)` | 挂起到被恢复；**超时则在单元内抛 `FiberTimeoutException`** |
| `sleep(Duration)` | 挂起到截止；截止到达是**正常唤醒**（非失败）；取消会中断它 |
| `join(child[, timeout])` | 等待直接子单元终态；可带超时 |
| `joinAll([timeout])` | 等待本单元全部子单元终态；可带超时 |
| `call(task)` | spawn 子单元并 join，返回其结果（结构化调用） |
| `fiber.await([timeout])` | **非 fiber 调用线程**使用；按墙钟超时，超时不取消该单元 |

挂起通过抛出内部 `SuspendSignal`（一个不采集栈的 `Error`）把工作线程栈展开回调度循环，
因此 `catch (Exception)` 不会误吞它；恢复时由运行时统一分流"正常 / 超时 / 取消"。

挂起提交采用**两阶段**：挂起点只记录意图，待工作线程栈完全展开后由调度器原子提交，
保证一个单元绝不会在栈仍运行时被另一个工作线程并发重入。

---

## 6. 结构化并发与取消语义

### 结构化保证

- 父单元在任务体返回后，**只要还有未结束的子单元就保持 `WAITING`**，全部子单元终态后才完成；
- 取消父单元会**递归级联**取消所有后代（原因标记为 `PARENT`）；
- 子单元始终登记在父（或根 `FiberScope`）名下，**无法逃逸为孤儿**；
- `FiberScope` 是给普通调用线程用的结构化边界，`close()` 等待全部子单元结束。

### 取消

- `fiber.cancel([reason])` / `scope.cancel()` 是**协作式**的：仅设置取消请求；
  - 若单元正挂起，立即中断并重新入队，下一切片在挂起点抛 `FiberCancellationException`；
  - 若单元在运行，于下一个挂起点 / `checkCancelled()` 生效。
- 取消原因 `CancelReason`：`MANUAL`、`TIMEOUT`、`PARENT`、`SHUTDOWN`。
- **取消回调恰好执行一次**：`onCancel(Runnable)` 注册的回调在首次有效取消时触发一次；
  取消之后再注册的回调至多立即执行一次；重复 cancel 为幂等 no-op；回调自身抛异常被吞掉，
  不影响其它回调、单元或调用方。
- 终态后 cancel 不再改变状态。

### 子单元失败策略 `ChildFailurePolicy`

- `CANCEL_SIBLINGS`（默认，快速失败）：某子单元 `FAILED` 时取消其它存活兄弟与等待中的父；
  `FiberScope.close()` 随后上抛首个失败。
- `CONTINUE`：不取消兄弟、不取消父；父可在 `joinAll()` 后通过 `child.state()/failure()`
  查询各子结果；`FiberScope.close()` 不上抛。

无论哪种策略，父都必须等所有子单元终态后才能结束。

---

## 7. 超时与截止时间

- **单元整体截止**：`FiberOptions.timeout(...)`。截止到达若未终态，则以 `TIMEOUT` 原因取消
  并级联后代；可中断正挂起的单元。整体定时器与单次挂起定时器相互独立，挂起/恢复不会误清。
- **挂起点超时**：`park(Duration)`、`join/joinAll(Duration)`（超时在单元内表现为异常）；
  `sleep(Duration)`（截止为正常完成）。
- **逻辑时钟 vs 墙钟**：单元内部调度与定时由调度器 `Clock`（可手动驱动，保证确定性）计量；
  而调用方线程的 `fiber.await(Duration)` / `scope.close(Duration)` 始终按**墙钟**计量，
  不会因手动时钟不前进而永久阻塞调用线程。

超时竞争（谁先发生：完成 / 外部恢复 / 截止）由运行时在恢复切片统一裁决，见
`TimeoutTest`（整体超时、手动取消胜出、回调一次、定时 park、定时 join 竞争）。

---

## 8. 故障隔离

- 任务体抛出的任何 `Throwable` 都在工作线程边界被捕获 → 单元 `FAILED`，异常存入 `failure()`；
- 工作线程绝不因单元异常而死亡，调度器与其它单元继续运行（`FaultIsolationTest`）；
- 创建/调度一个即将失败的单元不会在调用线程抛错；失败仅在 `await()` 时按需重抛；
- 三态与计数器 `SchedulerStats{completed, failed, cancelled, ...}` 可查询。

---

## 9. 运行期资源配额

`FiberSchedulerConfig`：

| 配额 | 超限行为 |
| --- | --- |
| `maxLiveFibers`（存活单元总数） | 创建时抛 `FiberLimitException`，**不占名额、不登记父/作用域** |
| `defaultMaxStackDepth` / `FiberOptions.maxStackDepth`（单单元逻辑帧深） | `enterFrame()` 超深抛 `StackQuotaException`，该单元 `FAILED` |
| `maxParkedFibers`（同时挂起等待总数） | 挂起点抛 `ParkLimitException`，**不增加挂起计数**，该单元 `FAILED`，已挂起者不受影响 |

所有拒绝路径都保证已分配资源可回收、无泄漏（`QuotaTest` 验证名额在单元结束后释放可复用）。

---

## 10. 关闭

`shutdown()` / `close()`：拒绝新建单元，以 `SHUTDOWN` 取消全部存活单元（级联后代），
等待工作线程排空退出；幂等、可重复调用、不抛异常。

---

## 11. 主要类型

- `FiberScheduler` / `FiberSchedulerConfig`：运行时与配置（工作线程数、时钟、老化参数、配额）。
- `Fiber<T>` / `FiberImpl`：执行单元句柄与堆上续体实现。
- `FiberContext`：任务体内的挂起/派生/等待/取消/帧计数 API；`FiberLocals` 堆上局部表。
- `FiberScope`：普通线程的结构化并发边界（`AutoCloseable`）。
- `FiberOptions` / `Priority` / `FiberState` / `CancelReason` / `ChildFailurePolicy`。
- `Clock` / `ManualClock` / `SystemClock` / `TimerWheel` / `ReadyQueue` / `SchedulerStats`。
- 异常：`FiberException` 及 `FiberCancellationException`、`FiberTimeoutException`、
  `FiberLimitException`、`StackQuotaException`、`ParkLimitException`。

---

## 12. 测试

```
mvn test
```

| 测试类 | 覆盖 |
| --- | --- |
| `SmokeTest` | 创建/挂起/恢复/局部状态保留；逻辑时钟 sleep |
| `StructuredConcurrencyTest` | 父等待全部子、父子孙级联取消、取消原因与幂等、无孤儿 |
| `TimeoutTest` | 整体超时中断挂起、手动/超时竞争、回调恰好一次、定时 park、定时 join 竞争 |
| `FaultIsolationTest` | 完成/失败/取消三态、异常不扩散、兄弟与调度器存活、失败可查询 |
| `QuotaTest` | 存活数 / 单栈帧深 / 挂起总数上限、拒绝不泄漏、名额释放复用 |
| `SchedulingDeterminismTest` / `ReadyQueueAgingTest` | 优先级+FIFO 可复现、老化防饿死 |
| `LifecycleTest` | shutdown 级联与排空、await 墙钟超时、异常不到达调用线程 |
| `MultiWorkerTest` | 多工作线程真实并发：2000 单元、结构化树、无孤儿 |
