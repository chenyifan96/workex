# Workex 限流算法说明

本项目实现了两种优先级队列限流算法。

---

## 📦 算法概览

| 算法 | 特性 | 适用场景 |
|------|------|----------|
| **PriorityAggregate** | 基本三优先级队列 | 简单优先级排序 |
| **AgedPriorityAggregate** | 带年龄过期的三优先级队列 | 时效性要求高的系统 |

---

## 1️⃣  PriorityAggregate - 基本优先级队列

### 核心特性

- ✅ 三个优先级：高、中、低
- ✅ 按优先级顺序处理：高 → 中 → 低
- ✅ FIFO：同优先级按先进先出
- ❌ 无过期机制：消息永久保留

### API

```elixir
# 创建队列
aggregate = Workex.PriorityAggregate.new()

# 添加消息
{:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "重要消息"})
{:ok, aggregate} = PriorityAggregate.add(aggregate, {:medium, "普通消息"})
{:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "日志信息"})

# 取出一条消息
{{:value, {priority, message}}, new_aggregate} = PriorityAggregate.pop(aggregate)

# 批量取出所有消息
{messages, new_aggregate} = PriorityAggregate.value(aggregate)

# 获取队列大小
size = PriorityAggregate.size(aggregate)

# 获取统计信息
stats = PriorityAggregate.stats(aggregate)
# %{high: 5, medium: 10, low: 20, total: 35}
```

### 适用场景

1. **任务队列**：优先处理重要任务
2. **消息推送**：VIP用户消息优先
3. **资源调度**：按优先级分配资源
4. **简单限流**：控制处理速率

### 完整集成示例

```bash
# 与 Workex 完整集成演示
mix run examples/priority_worker_demo.exs
```

演示内容：
- 场景1：基本使用 - 按优先级处理消息
- 场景2：限流应用 - 每秒20条
- 场景3：任务调度 - 优先执行重要任务
- 场景4：动态负载 - 监控队列状态

### 限制

⚠️ **可能出现低优先级饥饿**：如果高优先级消息持续不断，低优先级消息永远得不到处理

---

## 2️⃣  AgedPriorityAggregate - 带年龄过期的优先级队列

### 核心特性

- ✅ 三个优先级：高、中、低
- ✅ 消息年龄机制：防止低优先级饥饿
- ✅ 自动过期：年龄超过阈值的消息被丢弃
- ✅ 自动清理：过期消息过多时自动清理
- ✅ 防止计数器无限增长：队列为空时自动重置

### 年龄机制

```
年龄 = processed_count - birth_count + 1

• birth_count: 消息进入队列时的 processed_count
• processed_count: 已处理的消息总数
• 每处理一条消息，processed_count +1
• 所有未处理消息的年龄随之增长
```

**示例**：

```elixir
# max_age = 20（窗口大小为20条）
aggregate = AgedPriorityAggregate.new(max_age: 20)

# 添加低优先级消息（年龄 = 1）
{:ok, agg} = AgedPriorityAggregate.add(aggregate, {:low, "低优先级"})

# 处理20条高优先级消息
# → processed_count 从 0 增长到 20
# → 低优先级消息年龄从 1 增长到 21

# 取出低优先级消息 → 过期！（21 > 20）
{{:expired, {priority, message, age}}, _} = AgedPriorityAggregate.pop(agg)
```

### API

```elixir
# 创建队列（设置窗口大小）
aggregate = Workex.AgedPriorityAggregate.new(max_age: 20)

# 添加消息（自动触发清理检查）
{:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "消息"})

# 取出消息（自动丢弃过期）
{{:value, {priority, message}}, new_aggregate} = AgedPriorityAggregate.pop(aggregate)
# 或
{{:expired, {priority, message, age}}, new_aggregate} = AgedPriorityAggregate.pop(aggregate)

# 批量取出（自动处理过期）
{valid_messages, expired_count, new_aggregate} = 
  AgedPriorityAggregate.pop_batch(aggregate, 10)

# 手动清理所有过期消息
{expired_count, new_aggregate} = AgedPriorityAggregate.cleanup_expired(aggregate)

# 检查并自动清理
{cleaned?, expired_count, new_aggregate} = AgedPriorityAggregate.maybe_cleanup(aggregate)

# 获取统计信息（含过期比例）
stats = AgedPriorityAggregate.stats(aggregate)
# %{
#   high: 5, medium: 10, low: 20, total: 35,
#   max_age: 20, processed_count: 100,
#   estimated_expired: 15, expired_ratio: 0.43
# }
```

### 自动清理触发条件

满足任一条件即触发：

**策略 1（快速）**：
```
total_size > max_age * 2  AND  processed_count > max_age
```

**策略 2（精确）**：
```
total_size > 100  AND  采样过期比例 > 50%
```

### 重置机制

```elixir
# 问题：processed_count 会无限增长
# 解决：队列为空时自动重置为 0

# 处理一批消息
aggregate = ... # processed_count = 1000

# 处理完所有消息
{:empty, aggregate} = AgedPriorityAggregate.pop(aggregate)
# aggregate.processed_count == 0  ✅ 自动重置！
```

### 适用场景

1. **实时消息推送**：防止旧消息堆积
2. **任务调度系统**：保证任务时效性
3. **限流控制**：窗口内的流量控制
4. **实时数据处理**：丢弃过时数据
5. **防止饥饿**：确保低优先级消息不会永远等待

### 完整集成示例

```bash
# 与 Workex 完整集成演示
mix run examples/aged_priority_worker_demo.exs
```

演示内容：
- 场景1：基本使用 - 自动丢弃过期消息
- 场景2：防止饥饿 - 低优先级消息也能被处理
- 场景3：实时数据流 - 只处理最新数据
- 场景4：智能推送 - 限流 + 优先级 + 防过期

---

## 🎯 算法对比

| 特性 | PriorityAggregate | AgedPriorityAggregate |
|------|------------------|----------------------|
| **优先级** | ✅ 高、中、低 | ✅ 高、中、低 |
| **消息过期** | ❌ 永不过期 | ✅ 年龄过期 |
| **防止饥饿** | ❌ 可能饥饿 | ✅ 自动过期 |
| **自动清理** | ❌ 无 | ✅ 自动清理 |
| **重置机制** | ❌ 无需 | ✅ 队列空时重置 |
| **性能** | ⚡⚡⚡ 最快 | ⚡⚡ 快 |
| **内存** | 📦 小 | 📦 中等 |
| **复杂度** | 简单 | 中等 |

---

## 🧪 测试

```bash
# 运行所有测试
mix test

# 运行特定测试
mix test test/priority_aggregate_test.exs
mix test test/aged_priority_aggregate_test.exs
```

---

## 📚 完整集成演示

### PriorityAggregate + Workex

```bash
mix run examples/priority_worker_demo.exs
```

演示内容：
- 场景1：基本使用 - 按优先级处理消息
- 场景2：限流应用 - 每秒20条消息
- 场景3：任务调度 - 优先执行重要任务
- 场景4：动态负载 - 监控队列状态

### AgedPriorityAggregate + Workex

```bash
mix run examples/aged_priority_worker_demo.exs
```

演示内容：
- 场景1：基本使用 - 自动丢弃过期消息
- 场景2：防止饥饿 - 低优先级也能被处理
- 场景3：实时数据流 - 只处理最新数据
- 场景4：智能推送 - 限流+优先级+防过期

---

## 🚀 选择建议

### 使用 PriorityAggregate

- ✅ 消息没有时效性要求
- ✅ 低优先级消息不会饥饿
- ✅ 需要最佳性能
- ✅ 简单的优先级排序

### 使用 AgedPriorityAggregate

- ✅ 消息有时效性要求
- ✅ 需要防止低优先级饥饿
- ✅ 实时系统
- ✅ 需要窗口内的流量控制

---

## 💡 实际应用示例

### 消息推送系统

```elixir
# 使用 AgedPriorityAggregate 防止旧消息堆积
defmodule PushService do
  use GenServer
  alias Workex.AgedPriorityAggregate
  
  def init(_) do
    # 窗口大小20条，超过20条未处理的消息会过期
    aggregate = AgedPriorityAggregate.new(max_age: 20)
    {:ok, aggregate}
  end
  
  def handle_cast({:push, priority, message}, aggregate) do
    {:ok, new_aggregate} = AgedPriorityAggregate.add(aggregate, {priority, message})
    
    # 立即尝试发送
    new_aggregate = process_messages(new_aggregate)
    
    {:noreply, new_aggregate}
  end
  
  defp process_messages(aggregate) do
    case AgedPriorityAggregate.pop(aggregate) do
      {{:value, {_priority, message}}, new_agg} ->
        send_push(message)
        process_messages(new_agg)
      
      {{:expired, {_priority, message, age}}, new_agg} ->
        Logger.warn("消息过期：#{inspect(message)}, 年龄=#{age}")
        process_messages(new_agg)
      
      {:empty, agg} ->
        agg
    end
  end
end
```

### 任务队列

```elixir
# 使用 PriorityAggregate 简单优先级调度
defmodule TaskQueue do
  use GenServer
  alias Workex.PriorityAggregate
  
  def init(_) do
    aggregate = PriorityAggregate.new()
    schedule_worker()
    {:ok, aggregate}
  end
  
  def handle_cast({:enqueue, priority, task}, aggregate) do
    {:ok, new_aggregate} = PriorityAggregate.add(aggregate, {priority, task})
    {:noreply, new_aggregate}
  end
  
  def handle_info(:process, aggregate) do
    new_aggregate = case PriorityAggregate.pop(aggregate) do
      {{:value, {_priority, task}}, new_agg} ->
        Task.start(fn -> execute(task) end)
        new_agg
      
      {:empty, agg} ->
        agg
    end
    
    schedule_worker()
    {:noreply, new_aggregate}
  end
  
  defp schedule_worker do
    Process.send_after(self(), :process, 50)  # 每50ms处理一条（20条/秒）
  end
end
```

---

## 📖 更多信息

- 查看源码：`lib/workex/priority_aggregate.ex`
- 查看源码：`lib/workex/aged_priority_aggregate.ex`
- 查看测试：`test/priority_aggregate_test.exs`
- 查看测试：`test/aged_priority_aggregate_test.exs`

---

**Happy coding! 🎉**

