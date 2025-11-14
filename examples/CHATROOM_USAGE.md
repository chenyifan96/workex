# 聊天室限流使用指南

## 场景需求

聊天室进程需要处理两种消息：
- **普通消息**：有高、中、低三种优先级，限流 20条/秒
- **通知消息**：无优先级，限流 10条/秒

## 解决方案

使用两个独立的 Worker 分别处理两种消息，实现独立限流。

## 实现步骤

### 1. 定义 Worker

```elixir
defmodule NormalMessageProcessor do
  use Workex

  def init(opts) do
    {:ok, %{parent: opts[:parent]}}
  end

  def handle(messages, state) do
    # ⭐ 限流控制：50ms/条 = 20条/秒
    Enum.each(messages, fn {priority, content} ->
      :timer.sleep(50)
      # 投递给用户
      deliver_to_user(content)
    end)

    {:ok, state}
  end
end

defmodule NotificationProcessor do
  use Workex

  def init(opts) do
    {:ok, %{parent: opts[:parent]}}
  end

  def handle(messages, state) do
    # ⭐ 限流控制：100ms/条 = 10条/秒
    Enum.each(messages, fn content ->
      :timer.sleep(100)
      # 投递给用户
      deliver_notification(content)
    end)

    {:ok, state}
  end
end
```

### 2. 创建聊天室进程

```elixir
defmodule Chatroom do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def send_normal_message(chatroom, priority, content) do
    GenServer.cast(chatroom, {:normal_message, priority, content})
  end

  def send_notification(chatroom, content) do
    GenServer.cast(chatroom, {:notification, content})
  end

  @impl true
  def init(_opts) do
    # 启动普通消息 Worker（限流 20条/秒）
    {:ok, normal_worker} = Workex.start_link(
      NormalMessageProcessor,
      [],
      aggregate: %Workex.AgedPriorityAggregate{max_age: 50},
      max_size: 100,
      replace_oldest: true
    )

    # 启动通知消息 Worker（限流 10条/秒）
    {:ok, notification_worker} = Workex.start_link(
      NotificationProcessor,
      [],
      aggregate: %Workex.Queue{},
      max_size: 50,
      replace_oldest: true
    )

    {:ok, %{
      normal_message_worker: normal_worker,
      notification_worker: notification_worker
    }}
  end

  @impl true
  def handle_cast({:normal_message, priority, content}, state) do
    Workex.push(state.normal_message_worker, {priority, content})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:notification, content}, state) do
    Workex.push(state.notification_worker, content)
    {:noreply, state}
  end
end
```

### 3. 使用示例

```elixir
# 启动聊天室
{:ok, chatroom} = Chatroom.start_link()

# 发送普通消息（带优先级）
Chatroom.send_normal_message(chatroom, :high, "重要消息")
Chatroom.send_normal_message(chatroom, :medium, "普通消息")
Chatroom.send_normal_message(chatroom, :low, "低优先级消息")

# 发送通知消息
Chatroom.send_notification(chatroom, "系统通知")
```

## 关键要点

1. **两个独立的 Worker**：分别处理普通消息和通知消息
2. **普通消息使用 AgedPriorityAggregate**：支持优先级排序
3. **通知消息使用 Queue**：FIFO 顺序处理
4. **限流速率控制**：通过 `handle/2` 中的 `:timer.sleep()` 实现
   - 普通消息：`50ms/条 = 20条/秒`
   - 通知消息：`100ms/条 = 10条/秒`
5. **max_size 保护**：防止队列无限增长
6. **replace_oldest: true**：队列满时自动删除最老的消息

## 优势

- ✅ 两种消息独立限流，互不影响
- ✅ 普通消息支持优先级排序
- ✅ 自动过期保护（AgedPriorityAggregate）
- ✅ 队列容量保护（max_size）
- ✅ 简单易用，只需定义 handle/2

## 完整示例

运行完整示例：
```bash
mix run examples/chatroom_rate_limit_demo.exs
```

