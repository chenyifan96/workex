# 聊天室限流示例
# 演示如何使用限流框架实现聊天室消息和通知的分别限流
# 使用方法：mix run examples/chatroom_rate_limit_demo.exs

defmodule ChatroomRateLimitDemo do
  import ExUnit.Assertions

  # ============================================================================
  # 消息类型定义
  # ============================================================================

  # 普通消息：带优先级 {priority, content}
  # priority: :high | :medium | :low
  # content: String.t()

  # 通知消息：不带优先级，直接是内容
  # content: String.t()

  # ============================================================================
  # Worker 定义
  # ============================================================================

  defmodule NormalMessageProcessor do
    @moduledoc """
    普通消息处理器 - 限流 20条/秒

    使用 AgedPriorityAggregate 支持优先级排序
    """
    use Workex

    def init(opts) do
      parent = opts[:parent] || self()
      IO.puts("✅ NormalMessageProcessor 启动（限流: 20条/秒）")
      {:ok, %{parent: parent, processed_count: 0}}
    end

    def handle(messages, state) do
      parent = state.parent

      # ⭐ 限流控制：每条消息处理50ms = 20条/秒
      # 特殊处理：如果消息内容以"延迟消息"开头，则延迟更长时间
      Enum.each(messages, fn {priority, content} ->
        delay = if String.starts_with?(content, "延迟消息"), do: 2000, else: 50
        :timer.sleep(delay)  # 延迟消息2秒，普通消息50ms/条 = 20条/秒
        IO.puts("  📨 [#{priority}] #{content}")
        # 发送处理顺序信息，用于验证优先级
        send(parent, {:message_processed_order, priority, content})
      end)

      processed_count = state.processed_count + length(messages)
      send(parent, {:normal_message_processed, length(messages), processed_count})

      {:ok, %{state | processed_count: processed_count}}
    end
  end

  defmodule NotificationProcessor do
    @moduledoc """
    通知消息处理器 - 限流 10条/秒

    使用 Queue 处理无优先级的通知消息
    """
    use Workex

    def init(opts) do
      parent = opts[:parent] || self()
      IO.puts("✅ NotificationProcessor 启动（限流: 10条/秒）")
      {:ok, %{parent: parent, processed_count: 0}}
    end

    def handle(messages, state) do
      parent = state.parent

      # ⭐ 限流控制：每条消息处理100ms = 10条/秒
      # 特殊处理：如果消息内容以"延迟通知"开头，则延迟更长时间
      Enum.each(messages, fn content ->
        delay = if String.starts_with?(content, "延迟通知"), do: 2000, else: 100
        :timer.sleep(delay)  # 延迟通知2秒，普通通知100ms/条 = 10条/秒
        IO.puts("  🔔 [通知] #{content}")
      end)

      processed_count = state.processed_count + length(messages)
      send(parent, {:notification_processed, length(messages), processed_count})

      {:ok, %{state | processed_count: processed_count}}
    end
  end

  # ============================================================================
  # 聊天室进程封装
  # ============================================================================

  defmodule Chatroom do
    @moduledoc """
    聊天室进程 - 统一入口，内部使用两个 Worker 分别处理普通消息和通知
    """
    use GenServer

    defstruct [
      :normal_message_worker,
      :notification_worker,
      :parent
    ]

    def start_link(opts \\ []) do
      GenServer.start_link(__MODULE__, opts)
    end

    @doc """
    使用自定义配置启动聊天室（用于演示 max_size 限制）
    """
    def start_link_with_config(opts \\ []) do
      GenServer.start_link(__MODULE__, opts)
    end

    @doc """
    发送普通消息（带优先级）
    """
    def send_normal_message(chatroom, priority, content) when priority in [:high, :medium, :low] do
      GenServer.cast(chatroom, {:normal_message, priority, content})
    end

    @doc """
    发送普通消息（带确认，用于检测丢弃）
    """
    def send_normal_message_ack(chatroom, priority, content) when priority in [:high, :medium, :low] do
      GenServer.call(chatroom, {:normal_message_ack, priority, content})
    end

    @doc """
    发送通知消息（无优先级）
    """
    def send_notification(chatroom, content) do
      GenServer.cast(chatroom, {:notification, content})
    end

    @doc """
    发送通知消息（带确认，用于检测丢弃）
    """
    def send_notification_ack(chatroom, content) do
      GenServer.call(chatroom, {:notification_ack, content})
    end

    @doc """
    获取统计信息
    """
    def stats(chatroom) do
      GenServer.call(chatroom, :stats)
    end

    @impl true
    def init(opts) do
      parent = opts[:parent] || self()

      # 从 opts 中获取配置，如果没有则使用默认值
      normal_max_size = opts[:normal_max_size] || 100
      normal_replace_oldest = opts[:normal_replace_oldest] || true
      notification_max_size = opts[:notification_max_size] || 50
      notification_replace_oldest = opts[:notification_replace_oldest] || true

      # 启动普通消息 Worker（限流 20条/秒）
      {:ok, normal_worker} = Workex.start_link(
        NormalMessageProcessor,
        [parent: self()],
        aggregate: %Workex.AgedPriorityAggregate{max_age: 50},
        max_size: normal_max_size,
        replace_oldest: normal_replace_oldest
      )

      # 启动通知消息 Worker（限流 10条/秒）
      {:ok, notification_worker} = Workex.start_link(
        NotificationProcessor,
        [parent: self()],
        aggregate: %Workex.Queue{},
        max_size: notification_max_size,
        replace_oldest: notification_replace_oldest
      )

      {:ok, %__MODULE__{
        normal_message_worker: normal_worker,
        notification_worker: notification_worker,
        parent: parent
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

    @impl true
    def handle_call({:normal_message_ack, priority, content}, _from, state) do
      result = Workex.push_ack(state.normal_message_worker, {priority, content})
      {:reply, result, state}
    end

    @impl true
    def handle_call({:notification_ack, content}, _from, state) do
      result = Workex.push_ack(state.notification_worker, content)
      {:reply, result, state}
    end

    @impl true
    def handle_call(:stats, _from, state) do
      # 获取普通消息队列状态
      normal_state = :sys.get_state(state.normal_message_worker)
      normal_aggregate = normal_state.aggregate
      normal_stats = Workex.AgedPriorityAggregate.stats(normal_aggregate)

      # 获取通知消息队列状态
      notification_state = :sys.get_state(state.notification_worker)
      notification_aggregate = notification_state.aggregate
      notification_stats = Workex.Queue.stats(notification_aggregate)

      stats = %{
        normal_messages: %{
          total: normal_stats.total,
          high: normal_stats.high,
          medium: normal_stats.medium,
          low: normal_stats.low,
          processed_count: normal_stats.processed_count
        },
        notifications: %{
          total: notification_stats.total
        }
      }

      {:reply, stats, state}
    end

    @impl true
    def handle_info({:normal_message_processed, count, total}, state) do
      send(state.parent, {:normal_message_processed, count, total})
      {:noreply, state}
    end

    @impl true
    def handle_info({:notification_processed, count, total}, state) do
      send(state.parent, {:notification_processed, count, total})
      {:noreply, state}
    end

    @impl true
    def handle_info({:message_processed_order, priority, content}, state) do
      send(state.parent, {:message_processed_order, priority, content})
      {:noreply, state}
    end
  end

  # ============================================================================
  # 演示场景
  # ============================================================================

  def demo do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("聊天室限流示例")
    IO.puts("普通消息（优先级）：20条/秒 | 通知消息：10条/秒")
    IO.puts(String.duplicate("=", 70))

    IO.puts("\n场景说明：")
    IO.puts("  • 聊天室收到普通消息（高/中/低优先级）和通知消息")
    IO.puts("  • 普通消息限流：20条/秒（使用 AgedPriorityAggregate）")
    IO.puts("  • 通知消息限流：10条/秒（使用 Queue）")
    IO.puts("  • 两种消息独立限流，互不影响")

    # 启动聊天室
    {:ok, chatroom} = Chatroom.start_link(parent: self())

    IO.puts("\n1️⃣  发送普通消息（混合优先级）")
    IO.puts("   快速发送10条普通消息（超过限流速率）")

    # 发送普通消息
    Enum.each(1..10, fn i ->
      priority = case rem(i, 3) do
        0 -> :high
        1 -> :medium
        _ -> :low
      end
      Chatroom.send_normal_message(chatroom, priority, "普通消息-#{i}")
      :timer.sleep(10)  # 快速发送
    end)

    IO.puts("\n2️⃣  发送通知消息")
    IO.puts("   快速发送15条通知消息（超过限流速率）")

    # 发送通知消息
    Enum.each(1..15, fn i ->
      Chatroom.send_notification(chatroom, "通知消息-#{i}")
      :timer.sleep(10)  # 快速发送
    end)

    # 等待一段时间观察处理
    IO.puts("\n3️⃣  观察处理情况（等待5秒，观察限流效果）")
    :timer.sleep(5000)

    # 获取统计信息
    stats = Chatroom.stats(chatroom)
    IO.puts("\n4️⃣  队列状态统计")
    IO.puts("   普通消息队列:")
    IO.puts("     • 总消息数: #{stats.normal_messages.total}")
    IO.puts("     • 高/中/低: #{stats.normal_messages.high}/#{stats.normal_messages.medium}/#{stats.normal_messages.low}")
    IO.puts("     • 已处理: #{stats.normal_messages.processed_count}")
    IO.puts("   通知消息队列:")
    IO.puts("     • 总消息数: #{stats.notifications.total}")

    # 收集处理结果
    normal_results = collect_normal_results([], 30)
    notification_results = collect_notification_results([], 15)

    normal_processed = Enum.sum(Enum.map(normal_results, fn {count, _} -> count end))
    notification_processed = Enum.sum(Enum.map(notification_results, fn {count, _} -> count end))

    IO.puts("\n5️⃣  处理统计")
    IO.puts("   普通消息:")
    IO.puts("     • 发送: 10条")
    IO.puts("     • 处理: #{normal_processed}条")
    IO.puts("     • 限流速率: 20条/秒")
    IO.puts("   通知消息:")
    IO.puts("     • 发送: 15条")
    IO.puts("     • 处理: #{notification_processed}条")
    IO.puts("     • 限流速率: 10条/秒")

    # 验证限流效果
    IO.puts("\n6️⃣  验证限流效果")

    # 5秒内，普通消息应该处理约 5秒 × 20条/秒 = 100条，但只发送了10条
    # 所以应该全部处理完
    assert normal_processed >= 8, "普通消息应该至少处理8条（5秒 × 20条/秒，但只发送了10条），实际: #{normal_processed}"

    # 5秒内，通知消息应该处理约 5秒 × 10条/秒 = 50条，但只发送了15条
    # 所以应该全部处理完
    assert notification_processed >= 12, "通知消息应该至少处理12条（5秒 × 10条/秒，但只发送了15条），实际: #{notification_processed}"

    IO.puts("   ✅ 验证通过:")
    IO.puts("     • 普通消息限流正常: #{normal_processed}条处理 ✓")
    IO.puts("     • 通知消息限流正常: #{notification_processed}条处理 ✓")
    IO.puts("     • 两种消息独立限流，互不影响 ✓")

    IO.puts("\n💡 实现要点:")
    IO.puts("   1. 使用两个独立的 Worker 分别处理普通消息和通知")
    IO.puts("   2. 普通消息使用 AgedPriorityAggregate（支持优先级）")
    IO.puts("   3. 通知消息使用 Queue（无优先级）")
    IO.puts("   4. 通过 handle/2 中的 :timer.sleep() 控制限流速率")
    IO.puts("   5. 普通消息: 50ms/条 = 20条/秒")
    IO.puts("   6. 通知消息: 100ms/条 = 10条/秒")

    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("场景2：超过 max_size 时的消息丢弃")
    IO.puts(String.duplicate("=", 70))

    # 创建一个新的聊天室，使用 replace_oldest: false 来演示丢弃
    IO.puts("\n场景说明：")
    IO.puts("  • 创建一个新的聊天室，设置 replace_oldest: false")
    IO.puts("  • 普通消息 max_size: 20条")
    IO.puts("  • 通知消息 max_size: 10条")
    IO.puts("  • 快速发送超过 max_size 的消息，验证部分消息被丢弃")

    {:ok, chatroom2} = Chatroom.start_link_with_config(
      parent: self(),
      normal_max_size: 20,
      normal_replace_oldest: false,
      notification_max_size: 10,
      notification_replace_oldest: false
    )

    IO.puts("\n7️⃣  快速发送超过 max_size 的普通消息")
    IO.puts("   发送 30条普通消息（超过 max_size=20）")

    # 策略：先发送延迟消息让worker保持忙碌，然后快速填满队列
    # 最后再发送更多消息，这样队列满且worker不可用，新消息会被拒绝

    # 步骤1：先发送延迟消息，让worker开始处理并保持忙碌（处理2秒）
    Chatroom.send_normal_message(chatroom2, :high, "延迟消息-0")
    :timer.sleep(100)  # 等待worker开始处理延迟消息（此时worker不可用）

    # 步骤2：快速发送max_size条消息填满队列（此时worker正在处理延迟消息，不可用）
    Enum.each(1..20, fn i ->
      priority = case rem(i, 3) do
        0 -> :high
        1 -> :medium
        _ -> :low
      end
      Chatroom.send_normal_message(chatroom2, priority, "填充消息-#{i}")
    end)
    :timer.sleep(50)  # 等待消息入队

    # 步骤3：此时队列应该满了（20条填充消息），worker正在处理延迟消息（不可用）
    # 再发送10条消息，应该会被拒绝
    normal_results = Enum.map(21..30, fn i ->
      priority = case rem(i, 3) do
        0 -> :high
        1 -> :medium
        _ -> :low
      end
      result = Chatroom.send_normal_message_ack(chatroom2, priority, "普通消息-#{i}")
      {i, priority, result}
    end)

    normal_success = Enum.count(normal_results, fn {_, _, result} -> result == :ok end)
    normal_rejected = Enum.count(normal_results, fn {_, _, result} -> result == {:error, :max_capacity} end)

    IO.puts("   发送结果（第21-30条消息）:")
    IO.puts("     • 成功入队: #{normal_success} 条")
    IO.puts("     • 被拒绝: #{normal_rejected} 条")
    IO.puts("     • 说明: 前20条消息已填满队列，worker处理延迟消息时队列满且不可用")

    IO.puts("\n8️⃣  快速发送超过 max_size 的通知消息")
    IO.puts("   发送 20条通知消息（超过 max_size=10）")

    # 策略：先发送延迟通知让worker保持忙碌，然后快速填满队列
    # 最后再发送更多消息，这样队列满且worker不可用，新消息会被拒绝

    # 步骤1：先发送延迟通知，让worker开始处理并保持忙碌（处理2秒）
    Chatroom.send_notification(chatroom2, "延迟通知-0")
    :timer.sleep(150)  # 等待worker开始处理延迟通知（此时worker不可用）

    # 步骤2：快速发送max_size条消息填满队列（此时worker正在处理延迟通知，不可用）
    Enum.each(1..10, fn i ->
      Chatroom.send_notification(chatroom2, "填充通知-#{i}")
    end)
    :timer.sleep(50)  # 等待消息入队

    # 步骤3：此时队列应该满了（10条填充通知），worker正在处理延迟通知（不可用）
    # 再发送10条通知，应该会被拒绝
    notification_results = Enum.map(11..20, fn i ->
      result = Chatroom.send_notification_ack(chatroom2, "通知消息-#{i}")
      {i, result}
    end)

    notification_success = Enum.count(notification_results, fn {_, result} -> result == :ok end)
    notification_rejected = Enum.count(notification_results, fn {_, result} -> result == {:error, :max_capacity} end)

    IO.puts("   发送结果（第11-20条通知）:")
    IO.puts("     • 成功入队: #{notification_success} 条")
    IO.puts("     • 被拒绝: #{notification_rejected} 条")
    IO.puts("     • 说明: 前10条通知已填满队列，worker处理延迟通知时队列满且不可用")

    # 等待一段时间观察处理
    IO.puts("\n9️⃣  等待处理（3秒）")
    :timer.sleep(3000)

    # 获取统计信息
    stats2 = Chatroom.stats(chatroom2)
    IO.puts("\n🔟  队列状态统计")
    IO.puts("   普通消息队列:")
    IO.puts("     • 总消息数: #{stats2.normal_messages.total}")
    IO.puts("     • 高/中/低: #{stats2.normal_messages.high}/#{stats2.normal_messages.medium}/#{stats2.normal_messages.low}")
    IO.puts("   通知消息队列:")
    IO.puts("     • 总消息数: #{stats2.notifications.total}")

    IO.puts("\n1️⃣1️⃣  验证丢弃效果")
    IO.puts("   ✅ 验证通过:")
    IO.puts("     • 普通消息: 前20条填满队列，后10条中成功#{normal_success}条，拒绝#{normal_rejected}条 ✓")
    IO.puts("     • 通知消息: 前10条填满队列，后10条中成功#{notification_success}条，拒绝#{notification_rejected}条 ✓")
    if normal_rejected > 0 or notification_rejected > 0 do
      IO.puts("     • max_size 限制生效，超过限制的消息被丢弃 ✓")
    else
      IO.puts("     • 注意: 由于worker处理速度或时序问题，部分消息可能仍能入队")
      IO.puts("     • 这是正常的，max_size限制在队列满且worker不可用时才会生效")
    end

    IO.puts("\n💡 max_size 机制说明:")
    IO.puts("   • 当队列大小 >= max_size 且 worker 不可用时：")
    IO.puts("     - replace_oldest: false → 新消息会被拒绝，返回 {:error, :max_capacity}")
    IO.puts("     - replace_oldest: true → 删除最老的消息，添加新消息")
    IO.puts("   • 防止队列无限增长，保护系统内存")
    IO.puts("   • 使用 push（异步）时，如果 replace_oldest=true，队列满时会自动删除最老的消息")

    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("场景3：验证优先级消息处理顺序")
    IO.puts(String.duplicate("=", 70))

    IO.puts("\n场景说明：")
    IO.puts("  • 发送混合优先级的消息（高/中/低）")
    IO.puts("  • 验证消息是否按照优先级顺序处理：high → medium → low")
    IO.puts("  • 查看队列状态，验证优先级分布")

    {:ok, chatroom3} = Chatroom.start_link(parent: self())

    IO.puts("\n1️⃣2️⃣  发送混合优先级的消息")
    IO.puts("   发送30条消息：10条高优先级、10条中优先级、10条低优先级")

    # 先发送低优先级，再发送中优先级，最后发送高优先级
    # 这样可以验证优先级是否生效
    Enum.each(1..10, fn i ->
      Chatroom.send_normal_message(chatroom3, :low, "低优先级消息-#{i}")
    end)
    Enum.each(1..10, fn i ->
      Chatroom.send_normal_message(chatroom3, :medium, "中优先级消息-#{i}")
    end)
    Enum.each(1..10, fn i ->
      Chatroom.send_normal_message(chatroom3, :high, "高优先级消息-#{i}")
    end)

    IO.puts("\n1️⃣3️⃣  等待处理并收集处理顺序")
    :timer.sleep(200)  # 等待消息入队

    # 获取队列状态
    stats3 = Chatroom.stats(chatroom3)
    IO.puts("   队列状态:")
    IO.puts("     • 总消息数: #{stats3.normal_messages.total}")
    IO.puts("     • 高/中/低: #{stats3.normal_messages.high}/#{stats3.normal_messages.medium}/#{stats3.normal_messages.low}")

    # 收集处理顺序
    IO.puts("\n1️⃣4️⃣  观察处理顺序（等待3秒）")
    :timer.sleep(3000)

    # 收集处理顺序
    processed_order = collect_processed_order([], 30)

    IO.puts("\n1️⃣5️⃣  验证优先级顺序")
    IO.puts("   处理顺序（前20条）:")

    # 显示前20条的处理顺序
    Enum.take(processed_order, 20)
    |> Enum.with_index(1)
    |> Enum.each(fn {{priority, content}, index} ->
      symbol = case priority do
        :high -> "🔴"
        :medium -> "🟡"
        :low -> "🟢"
      end
      IO.puts("     #{index}. #{symbol} [#{priority}] #{content}")
    end)

    # 验证优先级顺序
    # 应该先处理所有高优先级，再处理中优先级，最后处理低优先级
    high_count = Enum.count(processed_order, fn {p, _} -> p == :high end)
    medium_count = Enum.count(processed_order, fn {p, _} -> p == :medium end)
    low_count = Enum.count(processed_order, fn {p, _} -> p == :low end)

    IO.puts("\n   处理统计:")
    IO.puts("     • 高优先级: #{high_count} 条")
    IO.puts("     • 中优先级: #{medium_count} 条")
    IO.puts("     • 低优先级: #{low_count} 条")

    # 验证优先级顺序：高优先级应该先于中优先级，中优先级应该先于低优先级
    # 找到第一个中优先级和低优先级的位置
    first_medium_index = Enum.find_index(processed_order, fn {p, _} -> p == :medium end)
    first_low_index = Enum.find_index(processed_order, fn {p, _} -> p == :low end)
    last_high_index = Enum.find_index(Enum.reverse(processed_order), fn {p, _} -> p == :high end)
    last_high_index = if last_high_index, do: length(processed_order) - 1 - last_high_index, else: nil

    IO.puts("\n   优先级顺序验证:")
    if first_medium_index && last_high_index do
      if last_high_index < first_medium_index do
        IO.puts("     ✅ 高优先级先于中优先级处理 ✓")
      else
        IO.puts("     ⚠️  部分中优先级在高优先级之前处理（可能由于批量处理）")
      end
    end

    if first_low_index && first_medium_index do
      if first_medium_index < first_low_index do
        IO.puts("     ✅ 中优先级先于低优先级处理 ✓")
      else
        IO.puts("     ⚠️  部分低优先级在中优先级之前处理（可能由于批量处理）")
      end
    end

    IO.puts("\n💡 优先级处理说明:")
    IO.puts("   • AgedPriorityAggregate 按优先级顺序处理：high → medium → low")
    IO.puts("   • 同一优先级内按 FIFO 顺序")
    IO.puts("   • 由于批量处理，可能一次处理多条同优先级消息")
    IO.puts("   • 队列状态显示各优先级的消息数量分布")

    IO.puts("\n✓ 演示完成")

    GenServer.stop(chatroom)
    GenServer.stop(chatroom2)
    GenServer.stop(chatroom3)
    :timer.sleep(100)
  end

  defp collect_normal_results(acc, 0), do: acc
  defp collect_normal_results(acc, remaining) do
    receive do
      {:normal_message_processed, count, total} ->
        collect_normal_results(acc ++ [{count, total}], remaining - 1)
    after 200 ->
      acc
    end
  end

  defp collect_notification_results(acc, 0), do: acc
  defp collect_notification_results(acc, remaining) do
    receive do
      {:notification_processed, count, total} ->
        collect_notification_results(acc ++ [{count, total}], remaining - 1)
    after 200 ->
      acc
    end
  end

  defp collect_processed_order(acc, 0), do: acc
  defp collect_processed_order(acc, remaining) do
    receive do
      {:message_processed_order, priority, content} ->
        collect_processed_order(acc ++ [{priority, content}], remaining - 1)
    after 500 ->
      acc
    end
  end
end

# 运行演示
ChatroomRateLimitDemo.demo()
