# Workex 快速开始指南

快速上手 Workex 的两个限流算法。

---

## 🚀 快速运行

### 查看完整集成演示

```bash
# 1. 基本优先级队列演示
mix run examples/priority_worker_demo.exs

# 2. 带年龄过期的优先级队列演示
mix run examples/aged_priority_worker_demo.exs
```

### 运行测试

```bash
mix test
```

---

## 📦 两个核心算法

### 1. PriorityAggregate - 基本优先级队列

**特点**：
- ✅ 三优先级（高、中、低）
- ✅ 优先级排序
- ❌ 无过期机制

**使用场景**：
- 简单的任务队列
- VIP 用户优先
- 不关心时效性

**示例代码**：

```elixir
defmodule MyWorker do
  use Workex
  
  def init(state), do: {:ok, state}
  
  def handle(messages, state) do
    # messages 已按优先级排序
    Enum.each(messages, fn {priority, msg} ->
      IO.puts("[#{priority}] #{msg}")
    end)
    {:ok, state}
  end
end

# 启动
{:ok, worker} = Workex.start_link(
  MyWorker,
  %{},
  aggregate: %Workex.PriorityAggregate{}
)

# 发送消息
Workex.push(worker, {:high, "VIP消息"})
Workex.push(worker, {:low, "普通消息"})
```

### 2. AgedPriorityAggregate - 带年龄过期

**特点**：
- ✅ 三优先级（高、中、低）
- ✅ 自动过期机制
- ✅ 防止低优先级饥饿
- ✅ 自动清理

**使用场景**：
- 实时推送系统
- 实时监控数据
- 有时效性要求
- 防止消息过时

**示例代码**：

```elixir
defmodule RealTimeWorker do
  use Workex
  
  def init(state), do: {:ok, state}
  
  def handle(messages, state) do
    # messages 都是有效的（过期的已被丢弃）
    Enum.each(messages, fn {priority, msg} ->
      IO.puts("[#{priority}] #{msg}")
    end)
    {:ok, state}
  end
end

# 启动（窗口大小20）
{:ok, worker} = Workex.start_link(
  RealTimeWorker,
  %{},
  aggregate: %Workex.AgedPriorityAggregate{max_age: 20}
)

# 发送消息
Workex.push(worker, {:high, "实时消息"})
Workex.push(worker, {:low, "日志消息"})
```

---

## 🎯 如何选择？

### 使用 PriorityAggregate

```
✅ 消息没有时效性
✅ 简单的优先级排序
✅ 需要最佳性能
✅ 低优先级不会被饿死
```

### 使用 AgedPriorityAggregate

```
✅ 消息有时效性要求
✅ 需要防止低优先级饥饿
✅ 实时系统（数据、推送、监控）
✅ 需要窗口内流量控制
```

---

## 💡 核心概念

### PriorityAggregate

```
高优先级 ────┐
             ├──→ 按顺序处理
中优先级 ────┤
             │
低优先级 ────┘
```

### AgedPriorityAggregate

```
消息进入 (年龄=1)
    ↓
每处理一条，年龄+1
    ↓
年龄 > max_age？
    ├─ 是 → 自动丢弃 ❌
    └─ 否 → 正常处理 ✅
```

**年龄计算**：
```elixir
age = processed_count - birth_count + 1
```

**窗口概念**：
- `max_age = 20` 表示窗口大小为 20 条
- 如果处理了 20 条高优先级
- 低优先级消息年龄变为 21
- 自动过期

---

## 📚 详细文档

- **算法详解**：查看 [ALGORITHMS.md](./ALGORITHMS.md)
- **源码**：
  - `lib/workex/priority_aggregate.ex`
  - `lib/workex/aged_priority_aggregate.ex`
- **测试**：
  - `test/priority_aggregate_test.exs`
  - `test/aged_priority_aggregate_test.exs`

---

## 🔧 实际应用示例

### 消息推送系统

```elixir
defmodule PushService do
  use Workex
  
  def init(_), do: {:ok, %{sent: 0}}
  
  def handle(messages, state) do
    # 限流：每批次最多 10 条
    {to_send, _} = Enum.split(messages, 10)
    
    Enum.each(to_send, fn {priority, msg} ->
      send_push_notification(priority, msg)
    end)
    
    {:ok, %{state | sent: state.sent + length(to_send)}}
  end
end

# 启动（窗口大小 15）
{:ok, worker} = Workex.start_link(
  PushService,
  %{},
  aggregate: %Workex.AgedPriorityAggregate{max_age: 15}
)

# 发送
Workex.push(worker, {:high, "VIP充值成功"})
Workex.push(worker, {:medium, "订单更新"})
Workex.push(worker, {:low, "系统通知"})
```

### 任务调度系统

```elixir
defmodule TaskScheduler do
  use Workex
  
  def init(_), do: {:ok, %{}}
  
  def handle(tasks, state) do
    Enum.each(tasks, fn {priority, task} ->
      execute_task(priority, task)
    end)
    
    {:ok, state}
  end
  
  defp execute_task(:high, task) do
    # 紧急任务立即执行
    Task.start(fn -> urgent_execution(task) end)
  end
  
  defp execute_task(:medium, task) do
    # 普通任务正常执行
    Task.start(fn -> normal_execution(task) end)
  end
  
  defp execute_task(:low, task) do
    # 后台任务延迟执行
    Task.start(fn -> background_execution(task) end)
  end
end

# 启动
{:ok, scheduler} = Workex.start_link(
  TaskScheduler,
  %{},
  aggregate: %Workex.PriorityAggregate{}
)

# 提交任务
Workex.push(scheduler, {:high, "用户支付处理"})
Workex.push(scheduler, {:medium, "发送邮件"})
Workex.push(scheduler, {:low, "数据备份"})
```

---

## 🎉 开始使用

1. **查看演示**：
   ```bash
   mix run examples/priority_worker_demo.exs
   mix run examples/aged_priority_worker_demo.exs
   ```

2. **运行测试**：
   ```bash
   mix test
   ```

3. **阅读文档**：
   ```bash
   cat ALGORITHMS.md
   ```

4. **开始编码**：
   - 复制上面的示例代码
   - 根据需求选择算法
   - 实现你的 Worker

---

**Happy coding! 🚀**

