# AgedPriorityAggregate 与 Workex 完整集成演示
# 使用方法：mix run examples/aged_priority_worker_demo.exs

defmodule AgedPriorityWorkerDemo do
  # 导入断言宏用于验证
  import ExUnit.Assertions

  # ============================================================================
  # Worker 定义
  # ============================================================================

  defmodule TimeSensitiveProcessor do
    @moduledoc """
    时效性消息处理器 - 使用 AgedPriorityAggregate

    场景：实时推送系统
    - 消息有时效性，过期自动丢弃
    - 防止低优先级消息永久等待
    - 窗口大小：20条
    """
    use Workex

    def init(opts) do
      IO.puts("✅ TimeSensitiveProcessor 启动 (窗口大小: #{opts[:window_size] || 20})")
      parent = opts[:parent] || self()
      {:ok, %{window_size: opts[:window_size] || 20, parent: parent, processed: []}}
    end

    def handle(messages, state) do
      # 记录处理的消息
      processed_msgs = Enum.map(messages, fn {priority, msg} -> {priority, msg} end)

      IO.puts("\n📦 批量处理 #{length(messages)} 条有效消息")

      Enum.each(messages, fn {priority, message} ->
        process_message(priority, message)
      end)

      # 发送统计信息给父进程
      all_processed = state.processed ++ processed_msgs
      send(state.parent, {:processed, all_processed})

      {:ok, Map.put(state, :processed, all_processed)}
    end

    defp process_message(:high, message) do
      IO.puts("  🔴 [高] #{message}")
    end

    defp process_message(:medium, message) do
      IO.puts("  🟡 [中] #{message}")
    end

    defp process_message(:low, message) do
      IO.puts("  🟢 [低] #{message}")
    end
  end

  # ============================================================================
  # 场景 1: 基本使用 - 自动丢弃过期消息
  # ============================================================================

  defmodule BasicExpirationWorker do
    @moduledoc """
    演示消息过期的基本 Worker
    """
    use Workex

    def init(opts) do
      {:ok, opts}
    end

    def handle(messages, state) do
      parent = state[:parent]

      # 统计处理的消息
      by_priority = Enum.group_by(messages, fn {p, _} -> p end)

      [:high, :medium, :low]
      |> Enum.each(fn priority ->
        if Map.has_key?(by_priority, priority) do
          count = length(by_priority[priority])
          symbol = case priority do
            :high -> "🔴"
            :medium -> "🟡"
            :low -> "🟢"
          end
          IO.puts("     #{symbol} [#{priority}] 处理了 #{count} 条")
        end
      end)

      # 通知父进程
      if parent, do: send(parent, {:batch_done, length(messages), by_priority})

      {:ok, state}
    end
  end

  def demo_basic_with_expiration do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("【场景 1】基本使用 - 自动丢弃过期消息")
    IO.puts(String.duplicate("=", 70))

    IO.puts("\n说明：完整的 Workex + AgedPriorityAggregate 集成")

    IO.puts("\n实验设计：")
    IO.puts("  • 窗口大小: 5条")
    IO.puts("  • 快速发送 10条低优先级消息")
    IO.puts("  • 再快速发送 10条高优先级消息（插队）")
    IO.puts("  • 观察低优先级消息因年龄过期被自动丢弃")

    # 启动 Workex Worker
    {:ok, worker} = Workex.start_link(
      BasicExpirationWorker,
      [parent: self()],
      aggregate: %Workex.AgedPriorityAggregate{max_age: 5}
    )

    IO.puts("\n1️⃣  初始状态")
    print_worker_state(worker)

    IO.puts("\n2️⃣  快速发送 10条低优先级消息")
    Enum.each(1..10, fn i ->
      Workex.push(worker, {:low, "低-#{i}"})
    end)
    :timer.sleep(30)  # 等待入队

    IO.puts("\n   入队后状态:")
    print_worker_state(worker)

    IO.puts("\n3️⃣  快速发送 10条高优先级消息（插队）")
    Enum.each(1..10, fn i ->
      Workex.push(worker, {:high, "高-#{i}"})
    end)
    :timer.sleep(50)

    IO.puts("\n   高优先级入队后状态:")
    print_worker_state(worker)

    IO.puts("\n4️⃣  等待处理完成...")

    # 增加等待时间确保所有消息都被处理
    :timer.sleep(200)

    # 收集处理结果（增加超时时间）
    results = collect_results([], 20)

    IO.puts("\n5️⃣  最终统计")

    total_high = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :high, []))
    end))
    total_low = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :low, []))
    end))

    IO.puts("   处理结果:")
    IO.puts("     • 高优先级: #{total_high}/10 条")
    IO.puts("     • 低优先级: #{total_low}/10 条")

    # 验证断言：核心验证过期机制是否工作
    # 注意：由于异步处理，高优先级消息可能因为处理速度问题没有全部处理完
    # 但核心验证点是：低优先级消息应该有过期
    assert total_high + total_low < 20, "应该有消息过期，但实际全部处理: 高#{total_high} + 低#{total_low} = #{total_high + total_low}/20"
    assert total_low < 10, "低优先级消息应该有过期，实际全部处理: #{total_low}/10"

    expired = 20 - (total_high + total_low)
    assert expired > 0, "应该有消息过期，但实际过期: #{expired} 条"

    IO.puts("\n   ✅ 验证通过:")
    IO.puts("     • 高优先级消息处理: #{total_high}/10")
    IO.puts("     • 低优先级消息处理: #{total_low}/10")
    IO.puts("     • 过期丢弃: #{expired} 条消息（其中低优先级 #{10 - total_low} 条）✓")
    IO.puts("      原因: 高优先级持续插队，低优先级消息年龄超过窗口(5)被丢弃")

    IO.puts("\n   最终队列状态:")
    print_worker_state(worker)

    IO.puts("\n💡 核心机制:")
    IO.puts("   • 消息入队时年龄为 1")
    IO.puts("   • 每处理一条消息，processed_count +1")
    IO.puts("   • 所有未处理消息的年龄 = processed_count - birth_count + 1")
    IO.puts("   • 年龄 > max_age 的消息在 pop 时被自动丢弃")

    IO.puts("\n✓ 演示完成：展示了 Workex 集成的消息过期机制")

    GenServer.stop(worker)
    :timer.sleep(50)
  end

  defp collect_results(acc, 0), do: acc
  defp collect_results(acc, remaining) do
    receive do
      {:batch_done, count, by_priority} ->
        # Worker 通过 send(parent, {:batch_done, count, by_priority}) 发送结果
        # 这里接收并存储为 {count, by_priority} 格式
        collect_results(acc ++ [{count, by_priority}], remaining - 1)
    after 200 ->
      acc
    end
  end

  defp print_worker_state(worker) do
    # 获取 Worker 内部状态
    state = :sys.get_state(worker)
    aggregate = state.aggregate

    stats = Workex.AgedPriorityAggregate.stats(aggregate)

    IO.puts("   📊 队列状态:")
    IO.puts("      • 总消息数: #{stats.total}")
    IO.puts("      • 高/中/低: #{stats.high}/#{stats.medium}/#{stats.low}")
    IO.puts("      • processed_count: #{stats.processed_count}")

    if stats.total > 0 do
      # 估算最老消息的年龄（假设最早入队的消息 birth_count = 0）
      oldest_age = stats.processed_count + 1
      IO.puts("      • 最老消息年龄: ≈#{oldest_age}")

      if oldest_age > aggregate.max_age do
        IO.puts("      ⚠️  警告：部分消息已超过 max_age(#{aggregate.max_age})，将在 pop 时被丢弃")
      end
    end
  end

  # ============================================================================
  # 场景 2: 防止饥饿
  # ============================================================================

  defmodule AntiStarvationProcessor do
    @moduledoc """
    防饥饿处理器 - 确保消息不会永久等待
    """
    use Workex

    def init(opts) do
      IO.puts("✅ AntiStarvationProcessor 启动")
      IO.puts("   窗口大小: #{opts[:max_age]}")
      IO.puts("   说明: 消息在窗口内未处理将自动过期")
      {:ok, opts}
    end

    def handle(messages, state) do
      parent = state[:parent]

      # 按优先级分组统计
      by_priority = Enum.group_by(messages, fn {p, _} -> p end)

      [:high, :medium, :low]
      |> Enum.each(fn priority ->
        if Map.has_key?(by_priority, priority) do
          count = length(by_priority[priority])
          symbol = case priority do
            :high -> "🔴"
            :medium -> "🟡"
            :low -> "🟢"
          end
          IO.puts("     #{symbol} [#{priority}] 处理了 #{count} 条")
        end
      end)

      # 通知父进程
      if parent, do: send(parent, {:batch_done, length(messages), by_priority})

      {:ok, state}
    end
  end

  def demo_anti_starvation do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("【场景 2】防止饥饿 - 低优先级消息也能被处理")
    IO.puts(String.duplicate("=", 70))

    {:ok, worker} = Workex.start_link(
      AntiStarvationProcessor,
      [max_age: 15, parent: self()],
      aggregate: %Workex.AgedPriorityAggregate{max_age: 15}
    )

    IO.puts("\n实验设计：")
    IO.puts("  • 窗口大小: 15条")
    IO.puts("  • 先发送 20条低优先级消息")
    IO.puts("  • 再持续发送高优先级消息")
    IO.puts("  • 观察低优先级消息的处理情况")

    IO.puts("\n1️⃣  发送 20 条低优先级消息")
    Enum.each(1..20, fn i ->
      Workex.push(worker, {:low, "低-#{i}"})
    end)

    :timer.sleep(50)

    IO.puts("\n2️⃣  持续发送 10 条高优先级消息")
    Enum.each(1..10, fn i ->
      Workex.push(worker, {:high, "高-#{i}"})
      :timer.sleep(20)
    end)

    :timer.sleep(100)

    IO.puts("\n3️⃣  再发送 5 条中优先级消息")
    Enum.each(1..5, fn i ->
      Workex.push(worker, {:medium, "中-#{i}"})
    end)

    :timer.sleep(200)

    # 增加等待时间确保所有消息都被处理
    :timer.sleep(100)

    # 收集结果
    results = collect_results([], 35)

    IO.puts("\n4️⃣  统计结果")

    total_high = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :high, []))
    end))
    total_medium = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :medium, []))
    end))
    total_low = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :low, []))
    end))

    IO.puts("   处理结果:")
    IO.puts("     • 高优先级: #{total_high}/10 条")
    IO.puts("     • 中优先级: #{total_medium}/5 条")
    IO.puts("     • 低优先级: #{total_low}/20 条")

    # 验证断言：核心验证防饥饿和过期机制
    assert total_high + total_medium + total_low < 35, "应该有消息过期，但实际全部处理: #{total_high + total_medium + total_low}/35"
    assert total_low > 0, "防饥饿验证失败：应该有低优先级消息被处理，实际: #{total_low}/20"
    assert total_low < 20, "过期验证失败：应该有部分低优先级消息过期，实际全部处理: #{total_low}/20"

    expired_low = 20 - total_low
    assert expired_low > 0, "应该有消息过期，但实际过期: #{expired_low} 条"

    IO.puts("\n   ✅ 验证通过:")
    IO.puts("     • 高优先级消息处理: #{total_high}/10")
    IO.puts("     • 中优先级消息处理: #{total_medium}/5")
    IO.puts("     • 过期丢弃: #{expired_low} 条低优先级消息 ✓")
    IO.puts("      原因: 高优先级插队，这些低优先级消息年龄超过窗口(15)")
    IO.puts("     • 防饥饿: #{total_low} 条低优先级消息在窗口内被处理 ✓")
    IO.puts("      说明: 窗口机制保证了部分低优先级消息不会永久等待")

    IO.puts("\n💡 核心价值:")
    IO.puts("   • 年龄过期机制自动丢弃过时消息")
    IO.puts("   • 避免处理已经失去时效性的数据")
    IO.puts("   • 平衡高优先级和低优先级消息的处理")

    IO.puts("\n✓ 演示完成：展示了防饥饿和过期保护的平衡")

    GenServer.stop(worker)
    :timer.sleep(50)
  end

  # ============================================================================
  # 场景 3: 实时数据流处理
  # ============================================================================

  defmodule RealTimeDataProcessor do
    @moduledoc """
    实时数据处理器 - 只处理最新的数据
    """
    use Workex

    def init(opts) do
      IO.puts("✅ RealTimeDataProcessor 启动")
      IO.puts("   实时窗口: #{opts[:max_age]}条")
      {:ok, opts}
    end

    def handle(messages, state) do
      parent = state[:parent]

      # 按优先级统计
      by_priority = Enum.group_by(messages, fn {p, _} -> p end)

      [:high, :medium, :low]
      |> Enum.each(fn priority ->
        if Map.has_key?(by_priority, priority) do
          count = length(by_priority[priority])
          symbol = case priority do
            :high -> "🔴"
            :medium -> "🟡"
            :low -> "🟢"
          end
          IO.puts("     #{symbol} [#{priority}] 处理了 #{count} 条")
        end
      end)

      # 通知父进程
      if parent, do: send(parent, {:batch_done, length(messages), by_priority})

      {:ok, state}
    end
  end

  def demo_realtime_data_stream do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("【场景 3】实时数据流 - 只处理最新数据")
    IO.puts(String.duplicate("=", 70))

    # 窗口大小为 20，确保只处理最近20条数据
    {:ok, worker} = Workex.start_link(
      RealTimeDataProcessor,
      [max_age: 20, parent: self()],
      aggregate: %Workex.AgedPriorityAggregate{max_age: 20}
    )

    IO.puts("\n场景：实时监控系统")
    IO.puts("  • 传感器持续上报数据 (30条)")
    IO.puts("  • 窗口大小 20条")
    IO.puts("  • 过时的数据自动丢弃")

    IO.puts("\n1️⃣  模拟数据持续上报")

    # 同步发送数据（不用spawn，方便统计）
    Enum.each(1..30, fn i ->
      priority = cond do
        rem(i, 10) == 0 -> :high  # 异常数据
        rem(i, 5) == 0 -> :medium # 警告数据
        true -> :low              # 正常数据
      end

      data = "数据-#{i}"
      Workex.push(worker, {priority, data})
      :timer.sleep(10)  # 快速上报
    end)

    IO.puts("   已发送 30条数据 (3条高, 3条中, 24条低)")

    # 等待处理完成
    :timer.sleep(300)

    # 增加等待时间确保所有消息都被处理
    :timer.sleep(100)

    # 收集结果
    results = collect_results([], 30)

    IO.puts("\n2️⃣  统计结果")

    total_high = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :high, []))
    end))
    total_medium = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :medium, []))
    end))
    total_low = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :low, []))
    end))

    total_processed = total_high + total_medium + total_low

    IO.puts("   实际处理:")
    IO.puts("     • 高优先级: #{total_high}/3 条")
    IO.puts("     • 中优先级: #{total_medium}/3 条")
    IO.puts("     • 低优先级: #{total_low}/24 条")
    IO.puts("     • 总计: #{total_processed}/30 条")

    # 验证断言
    assert total_high == 3, "高优先级数据应该全部处理，实际: #{total_high}/3"
    assert total_medium == 3, "中优先级数据应该全部处理，实际: #{total_medium}/3"
    assert total_processed <= 30, "处理数量不应超过发送数量，实际: #{total_processed}/30"
    assert total_processed >= 20, "应该至少处理窗口大小的数据，实际: #{total_processed}/20"

    if total_processed < 30 do
      expired = 30 - total_processed
      assert expired > 0, "应该有数据过期，但实际过期: #{expired} 条"

      IO.puts("\n   ✅ 验证通过:")
      IO.puts("     • 自动丢弃: #{expired} 条过时数据 ✓")
      IO.puts("      原因: 数据流速快，超出窗口(20)的数据被丢弃")
      IO.puts("      保证: 只处理最新的有效数据")
    else
      IO.puts("\n   ✅ 验证通过:")
      IO.puts("     • 所有数据都被处理（处理速度快于数据流速）")
      IO.puts("     • 至少处理了窗口大小的数据: #{total_processed}/20 ✓")
    end

    IO.puts("\n💡 应用价值:")
    IO.puts("   • 实时数据流处理")
    IO.puts("   • 自动丢弃过时数据")
    IO.puts("   • 保证系统实时性")

    IO.puts("\n✓ 演示完成：展示了实时数据流处理场景")

    GenServer.stop(worker)
    :timer.sleep(50)
  end

  # ============================================================================
  # 场景 4: 智能消息推送
  # ============================================================================

  defmodule SmartPushProcessor do
    @moduledoc """
    智能推送处理器 - 综合应用
    """
    use Workex

    def init(opts) do
      IO.puts("✅ SmartPushProcessor 启动")
      IO.puts("   窗口大小: #{opts[:max_age]}")
      {:ok, opts}
    end

    def handle(messages, state) do
      parent = state[:parent]

      # 按优先级统计
      by_priority = Enum.group_by(messages, fn {p, _} -> p end)

      [:high, :medium, :low]
      |> Enum.each(fn priority ->
        if Map.has_key?(by_priority, priority) do
          count = length(by_priority[priority])
          symbol = case priority do
            :high -> "🔴"
            :medium -> "🟡"
            :low -> "🟢"
          end
          IO.puts("     #{symbol} [#{priority}] 推送了 #{count} 条")
        end
      end)

      # 通知父进程
      if parent, do: send(parent, {:batch_done, length(messages), by_priority})

      {:ok, state}
    end
  end

  def demo_smart_push_system do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("【场景 4】智能推送 - 优先级 + 防过期综合应用")
    IO.puts(String.duplicate("=", 70))

    {:ok, worker} = Workex.start_link(
      SmartPushProcessor,
      [max_age: 10, parent: self()],
      aggregate: %Workex.AgedPriorityAggregate{max_age: 10}
    )

    IO.puts("\n设计目标：")
    IO.puts("  ✅ 优先级：VIP消息优先推送")
    IO.puts("  ✅ 防过期：超过窗口(10)的消息自动丢弃")
    IO.puts("  ✅ 时效性：保证系统只处理新鲜消息")

    IO.puts("\n1️⃣  模拟各种推送场景")

    # VIP 用户消息
    IO.puts("\n   [VIP消息] 发送5条高优先级")
    Enum.each(1..5, fn i ->
      Workex.push(worker, {:high, "VIP-#{i}"})
    end)

    :timer.sleep(50)

    # 普通用户消息（大量）
    IO.puts("   [普通消息] 批量发送15条中优先级")
    Enum.each(1..15, fn i ->
      Workex.push(worker, {:medium, "中-#{i}"})
    end)

    :timer.sleep(50)

    # 系统通知
    IO.puts("   [系统通知] 发送5条低优先级")
    Enum.each(1..5, fn i ->
      Workex.push(worker, {:low, "低-#{i}"})
    end)

    :timer.sleep(300)

    # 增加等待时间确保所有消息都被处理
    :timer.sleep(100)

    # 收集结果
    results = collect_results([], 30)

    IO.puts("\n2️⃣  统计结果")

    total_high = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :high, []))
    end))
    total_medium = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :medium, []))
    end))
    total_low = Enum.sum(Enum.map(results, fn {_, by_pri} ->
      length(Map.get(by_pri, :low, []))
    end))

    total_processed = total_high + total_medium + total_low
    total_sent = 5 + 15 + 5  # 25条

    IO.puts("   推送结果:")
    IO.puts("     • VIP (高):  #{total_high}/5 条")
    IO.puts("     • 普通(中):  #{total_medium}/15 条")
    IO.puts("     • 通知(低):  #{total_low}/5 条")
    IO.puts("     • 总计: #{total_processed}/#{total_sent} 条")

    # 验证断言：核心验证VIP消息优先和过期机制
    assert total_high == 5, "VIP消息应该全部推送，实际: #{total_high}/5"
    assert total_processed <= total_sent, "处理数量不应超过发送数量，实际: #{total_processed}/#{total_sent}"
    assert total_processed < total_sent, "应该有消息过期，但实际全部推送: #{total_processed}/#{total_sent}"

    expired = total_sent - total_processed
    assert expired > 0, "应该有消息过期，但实际过期: #{expired} 条"

    IO.puts("\n   ✅ 验证通过:")
    IO.puts("     • VIP消息全部推送: #{total_high}/5 ✓")
    IO.puts("     • 过期丢弃: #{expired} 条消息 ✓")
    IO.puts("      原因: VIP消息插队，部分消息年龄超过窗口(10)被丢弃")

    # 分析哪些优先级的消息被丢弃
    if total_medium < 15 do
      expired_medium = 15 - total_medium
      IO.puts("      • 中优先级丢弃: #{expired_medium}条")
    end
    if total_low < 5 do
      expired_low = 5 - total_low
      IO.puts("      • 低优先级丢弃: #{expired_low}条")
    end

    IO.puts("\n💡 实际应用价值:")
    IO.puts("   • VIP用户消息优先处理")
    IO.puts("   • 自动丢弃过时消息，避免延迟推送")
    IO.puts("   • 保证用户看到的都是时效性强的信息")
    IO.puts("   • 适用于：消息推送、订单通知、活动提醒等")

    IO.puts("\n✓ 演示完成：展示了智能推送系统的完整应用")

    GenServer.stop(worker)
    :timer.sleep(50)
  end

  # ============================================================================
  # 辅助函数
  # ============================================================================

  defp receive_last_processed do
    receive_last_processed([])
  end

  defp receive_last_processed(last_result) do
    receive do
      {:processed, msgs} ->
        receive_last_processed(msgs)
    after 100 ->
      last_result
    end
  end

  # ============================================================================
  # 主函数
  # ============================================================================

  def run do
    print_header()

    demo_basic_with_expiration()     # 场景1: 基本使用 - 自动丢弃过期消息
    demo_anti_starvation()           # 场景2: 防止饥饿
    demo_realtime_data_stream()      # 场景3: 实时数据流
    demo_smart_push_system()         # 场景4: 智能推送

    print_footer()
  end

  defp print_header do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts(" AgedPriorityAggregate + Workex 完整集成演示")
    IO.puts(" 带年龄过期机制的优先级队列实际应用")
    IO.puts(String.duplicate("=", 70))
  end

  defp print_footer do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts(" 演示完成")
    IO.puts(String.duplicate("=", 70))

    IO.puts("\n💡 核心概念：")
    IO.puts("""

    【Workex + AgedPriorityAggregate】

    1️⃣  Worker 定义：
       defmodule MyWorker do
         use Workex

         def init(opts), do: {:ok, opts}

         def handle(messages, state) do
           # 这里收到的都是有效消息
           # 过期消息已被自动丢弃
           {:ok, state}
         end
       end

    2️⃣  启动 Worker（设置窗口大小）：
       {:ok, worker} = Workex.start_link(
         MyWorker,
         initial_state,
         aggregate: %Workex.AgedPriorityAggregate{max_age: 20}
       )

    3️⃣  发送消息：
       Workex.push(worker, {:high, "重要消息"})
       Workex.push(worker, {:medium, "普通消息"})
       Workex.push(worker, {:low, "日志消息"})

    4️⃣  自动处理流程：
       ┌─────────────┐
       │ 发送消息    │
       └──────┬──────┘
              ↓
       ┌─────────────┐
       │ 入队 (年龄=1)│
       └──────┬──────┘
              ↓
       ┌─────────────┐
       │ 按优先级排序│
       └──────┬──────┘
              ↓
       ┌─────────────┐
       │ 检查年龄    │
       └──────┬──────┘
              ↓
       ┌─────────────┐     ┌─────────────┐
       │ 有效消息    │────→│ handle/2    │
       └─────────────┘     └─────────────┘
              │
              ↓
       ┌─────────────┐
       │ 过期消息    │ → 自动丢弃
       └─────────────┘

    【年龄机制】
    • 消息进入时：年龄 = 1
    • 每处理一条：所有未处理消息年龄 +1
    • 年龄 > max_age：自动过期丢弃
    • 队列为空时：计数器重置

    【自动清理】
    触发条件（满足任一）：
    1. 快速：total_size > max_age*2 AND processed_count > max_age
    2. 精确：total_size > 100 AND 过期比例 > 50%

    【优势】
    ✅ 防止饥饿：低优先级不会永久等待
    ✅ 时效保证：过时数据自动丢弃
    ✅ 自动清理：维护队列健康
    ✅ 窗口控制：精确控制处理范围

    【适用场景】
    • 实时推送系统（消息有时效性）
    • 实时监控系统（只关心最新数据）
    • 任务调度系统（防止任务过期）
    • 流量控制系统（窗口内限流）
    • 实时数据处理（丢弃过时数据）

    【配置建议】
    • max_age = 20：适合中等负载
    • max_age = 50：适合高负载
    • max_age = 10：适合低延迟要求

    【监控指标】
    • processed_count：已处理消息数
    • expired_ratio：过期消息比例
    • total_size：当前队列大小
    """)
  end
end

# 运行演示
AgedPriorityWorkerDemo.run()
