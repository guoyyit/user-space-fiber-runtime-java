# User-Space Fiber Runtime (Java 17)

一套运行在当前服务进程内的**用户态协作式并发运行时**。执行单元（fiber）
挂起时不占用任何执行线程，恢复后局部状态完整；支持结构化并发、超时与截止
时间、优先级与公平调度、故障隔离以及运行期资源配额。调度不依赖操作系统
级抢占：所有 fiber 体都在单个调度器线程上以协作方式推进到下一个挂起点。

## 快速开始

```java
try (FiberRuntime rt = FiberRuntime.builder().start()) {
    FiberHandle<String> h = rt.spawn(FiberOptions.DEFAULT
            .withPriority(FiberPriority.HIGH)
            .withTimeout(Duration.ofSeconds(5)), ctx -> {
        int a = ctx.call(() -> loadA());        // 记忆化副作用（只执行一次）
        ctx.sleep(Duration.ofMillis(100));       // 挂起：不占用任何线程
        FiberHandle<Integer> child = ctx.spawn(FiberOptions.named("child"),
                c -> 40);                        // 结构化子单元
        return "sum=" + (a + ctx.await(child));  // 等待子单元结果
    });
    String value = h.result();                   // 阻塞调用方直到完成
}
```

测试可用确定性逻辑时钟（时间瞬时推进、调度顺序可复现）：

```java
FiberRuntime rt = FiberRuntime.builder().virtualClock(new VirtualClock()).start();
```

## 执行模型：日志回放（journal / replay）

Java 17 没有栈式续体，因此本运行时采用**确定性回放**实现挂起/恢复：

- fiber 体（`FiberBody`）每次恢复时从头重新执行；
- 每个 `ctx.*` 操作按序取一个操作序号，已完成的结果记录在日志（journal）
  中，重放时直接返回记录值，**不会重复执行**；
- 遇到需要等待的操作（`sleep` / `await` / `park`）时，登记唤醒事件后通过
  控制异常展开栈——fiber 从此只是堆上的一个对象，**不占用执行线程**；
- 事件就绪后调度器重新派发，日志重放把局部状态确定性地重建到挂起点。

**契约**（与 Temporal 等工作流引擎相同）：

1. fiber 体必须确定性：同样的日志前缀必须走出同样的控制流；
2. 一切副作用必须包在 `ctx.call(Supplier)` 里（只执行一次，之后重放）；
3. 不要吞掉 `FiberCancellationException` 后继续走不同分支。

## 调度语义

- **单调度器线程**：所有 fiber 体协作式运行到下一个挂起点，无 OS 抢占，
  交错顺序完全确定。
- **就绪选择**：得分 = `基础优先级 × 10 + 等待龄期`，分高者优先，同分按
  提交序号（先到先得）。每派发一次，其余就绪 fiber 的龄期 +1。
- **公平性（抗饿死）**：龄期线性增长保证任何就绪 fiber 最多等待
  `优先级差 × 10` 个调度回合即获得执行（见 `FairnessTest`）。
- **定时器**：按截止时间排序的最小堆；空闲时系统时钟下真实等待、逻辑
  时钟下瞬时跳转，因此相同输入的调度顺序**可复现**
  （见 `DeterministicReplayTest`）。

## 取消语义

- `handle.cancel()` 是**协作式**请求：置位取消标志，级联到所有后代；
  若目标正挂起在某个操作上，该操作立即以取消异常完成，唤醒 fiber 并在
  重放到该点时向其体内抛出 `FiberCancellationException`。
- 正在运行的 fiber 在下一个 `ctx.*` 操作或 `checkCancelled()` 处收到取消。
- **取消回调**（`ctx.onCancel`）在 fiber 进入终态 `CANCELLED` 时执行，
  **恰好一次**；回调内异常被隔离，不影响调度器。
- 终态规则：未捕获的取消异常 → `CANCELLED`；未捕获的其他异常 →
  `FAILED`；正常返回但已请求取消 → `CANCELLED`；正常返回 → `COMPLETED`。
- **超时**：`FiberOptions.withTimeout` 在提交时登记截止时间，到期等价于
  以 `TimeoutCancellationException` 发起取消，可中断挂起中的单元。

## 结构化并发

- 父单元**必须等待全部子单元终止**才进入终态（体先返回则挂起等待）；
- 取消父单元**级联取消所有后代**；子单元无法逃逸为孤儿——运行时关闭时
  所有根单元被级联取消；
- 子单元失败默认不传染父单元，可通过 `ctx.await(child)` 显式获取并处理
  （失败以 `FiberFailedException` 重抛，可查询 `child.outcome()`）。

## 故障隔离

- 体内未捕获异常被调度器捕获并转为 `Outcome.Failed`，可通过
  `handle.outcome()` / `await()` 查询，`result()` 以
  `FiberFailedException` 重抛（携带原始 cause）；
- 单个 fiber 的失败不影响调度器和其他 fiber；三种终态
  （`COMPLETED` / `FAILED` / `CANCELLED`）可明确区分。

## 资源配额（`FiberQuotas`）

| 配额 | 语义 | 超限行为 |
| --- | --- | --- |
| `maxLiveFibers` | 非终态 fiber 总数 | `spawn` 抛出 `QuotaExceededException(LIVE_FIBERS)`，不分配任何资源 |
| `maxSuspendedFibers` | 同时挂起的 fiber 数 | 挂起操作在体内抛出 `QuotaExceededException(SUSPENDED_FIBERS)` |
| `maxStackDepth` | 挂起点处 fiber 体的 Java 栈深 | 挂起操作抛出 `QuotaExceededException(STACK_DEPTH)` |

所有计数在 fiber 进入终态时正确归还，拒绝路径不泄漏已分配资源
（见 `QuotaTest`）。

## REST 演示接口（服务进程内）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/fibers?name=&steps=&sleepMs=` | 创建一个按步挂起的演示 fiber |
| `GET` | `/api/fibers` | 列出所有非终态 fiber |
| `GET` | `/api/fibers/{id}` | 查询状态与终态结果 |
| `DELETE` | `/api/fibers/{id}` | 请求取消 |

配额可通过 `application.properties` 的 `fiber.quotas.*` 配置。

## 测试覆盖（26 个用例）

- `SmokeTest`：创建/运行/挂起/恢复/等待，副作用只执行一次；
- `StructuredConcurrencyTest`：父等子、级联取消、无孤儿、子失败可查询；
- `TimeoutTest`：超时中断挂起、回调恰好一次、完成与超时的竞争；
- `FairnessTest`：高频提交下低优先级不饿死、同优先级基础序；
- `FaultIsolationTest`：异常转为失败态、调度器与他人不受影响、三态区分；
- `QuotaTest`：三类配额拒绝与计数归还；
- `DeterministicReplayTest`：相同输入调度顺序完全一致、多挂起点状态完整；
- `FiberRuntimeApiTest`：Spring 进程内 REST 端到端。
