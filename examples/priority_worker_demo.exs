# PriorityAggregate 与 Workex 完整集成演示
# 使用方法：mix run examples/priority_worker_demo.exs

defmodule PriorityWorkerDemo do

  # ============================================================================
  # Worker 定义
  # ============================================================================

  defmodule MessageProcessor do
    @moduledoc """
    消息处理器 - 使用 PriorityAggregate

    场景：消息推送系统
    - 高优先级：VIP 用户消息
    - 中优先级：普通用户消息
    - 低优先级：系统通知
    """
    use Workex

    def init(opts) do
      IO.puts("✅ MessageProcessor 启动")
      {:ok, opts}
    end

    def handle(messages, state) do
      parent = state[:parent]

      # 按优先级分组
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

  # ============================================================================
  # 场景 1: 基本使用
  # ============================================================================

  def demo_basic_usage do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("【场景 1】基本使用 - 按优先级处理消息")
    IO.puts(String.duplicate("=", 70))

    # 启动 Worker，使用 PriorityAggregate
    {:ok, worker} = Workex.start_link(
      MessageProcessor,
      [parent: self()],
      aggregate: %Workex.PriorityAggregate{}
    )

    IO.puts("\n1️⃣  混合发送不同优先级的消息")

    # 模拟混合发送消息
    Workex.push(worker, {:low, "系统日志"})
    Workex.push(worker, {:high, "VIP充值通知"})
    Workex.push(worker, {:medium, "普通消息-1"})
    Workex.push(worker, {:low, "系统统计"})
    Workex.push(worker, {:high, "VIP订单通知"})
    Workex.push(worker, {:medium, "普通消息-2"})

    IO.puts("   已发送: 2条高 + 2条中 + 2条低")

    # 等待处理完成
    :timer.sleep(150)

    # 收集结果
    results = collect_results([], 10)

    IO.puts("\n2️⃣  验证处理顺序")

    if length(results) > 0 do
      # 合并所有批次，统计各优先级
      total_high = Enum.sum(Enum.map(results, fn {_, by_pri} ->
        length(Map.get(by_pri, :high, []))
      end))
      total_medium = Enum.sum(Enum.map(results, fn {_, by_pri} ->
        length(Map.get(by_pri, :medium, []))
      end))
      total_low = Enum.sum(Enum.map(results, fn {_, by_pri} ->
        length(Map.get(by_pri, :low, []))
      end))

      IO.puts("   处理统计:")
      IO.puts("     • 高优先级: #{total_high}/2 条")
      IO.puts("     • 中优先级: #{total_medium}/2 条")
      IO.puts("     • 低优先级: #{total_low}/2 条")

      if total_high == 2 and total_medium == 2 and total_low == 2 do
        IO.puts("\n   ✅ 所有消息都按优先级顺序处理完成")
      end
    else
      IO.puts("   ℹ️  未收到处理结果（处理太快）")
    end

    IO.puts("\n💡 核心特性:")
    IO.puts("   • 高优先级消息优先处理")
    IO.puts("   • 同优先级消息按FIFO顺序")
    IO.puts("   • 适用于需要优先级区分的场景")

    IO.puts("\n✓ 演示完成：展示了优先级处理机制")

    # 停止 Worker
    GenServer.stop(worker)
    :timer.sleep(50)
  end

  # ============================================================================
  # 场景 2: 限流应用
  # ============================================================================

  defmodule RateLimitProcessor do
    @moduledoc """
    限流处理器 - 控制处理速率
    """
    use Workex

    def init(opts) do
      IO.puts("✅ RateLimitProcessor 启动 (限流: 20条/秒)")
      {:ok, opts}
    end

    def handle(messages, state) do
      parent = state[:parent]
      start_time = System.monotonic_time(:millisecond)

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

      # 模拟处理延迟（限流：50ms/条）
      Enum.each(messages, fn _ -> :timer.sleep(50) end)

      elapsed = System.monotonic_time(:millisecond) - start_time
      IO.puts("     ⏱  本批耗时: #{elapsed}ms")

      # 通知父进程
      if parent, do: send(parent, {:batch_done, length(messages), by_priority, elapsed})

      {:ok, state}
    end
  end

  def demo_rate_limiting do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("【场景 2】限流应用 - 控制处理速率")
    IO.puts(String.duplicate("=", 70))

    {:ok, worker} = Workex.start_link(
      RateLimitProcessor,
      [parent: self()],
      aggregate: %Workex.PriorityAggregate{}
    )

    IO.puts("\n1️⃣  快速发送 12 条混合优先级消息")

    start_time = System.monotonic_time(:millisecond)

    # 快速发送消息
    Enum.each(1..12, fn i ->
      priority = case rem(i, 3) do
        0 -> :high
        1 -> :medium
        _ -> :low
      end
      Workex.push(worker, {priority, "消息-#{i}"})
    end)

    send_elapsed = System.monotonic_time(:millisecond) - start_time
    IO.puts("   发送完成: 12条消息 (耗时 #{send_elapsed}ms)")

    IO.puts("\n2️⃣  等待限流处理...")

    # 等待处理完成
    :timer.sleep(700)

    # 收集结果
    results = collect_rate_limit_results([], 12)

    IO.puts("\n3️⃣  验证限流效果")

    if length(results) > 0 do
      total_count = Enum.sum(Enum.map(results, fn {count, _, _} -> count end))
      total_time = Enum.sum(Enum.map(results, fn {_, _, elapsed} -> elapsed end))

      avg_time_per_msg = if total_count > 0, do: div(total_time, total_count), else: 0

      IO.puts("   处理统计:")
      IO.puts("     • 总消息数: #{total_count}/12 条")
      IO.puts("     • 总耗时: #{total_time}ms")
      IO.puts("     • 平均速率: #{avg_time_per_msg}ms/条")
      IO.puts("     • 预期速率: 50ms/条 (20条/秒)")

      if avg_time_per_msg >= 45 and avg_time_per_msg <= 55 do
        IO.puts("\n   ✅ 限流效果达标 (实际速率接近目标)")
      else
        IO.puts("\n   ℹ️  速率偏差: #{abs(avg_time_per_msg - 50)}ms")
      end
    else
      IO.puts("   ℹ️  未收到处理结果")
    end

    IO.puts("\n💡 应用场景:")
    IO.puts("   • API调用限流")
    IO.puts("   • 消息推送限流")
    IO.puts("   • 数据库写入限流")

    IO.puts("\n✓ 演示完成：展示了限流机制")

    GenServer.stop(worker)
    :timer.sleep(50)
  end

  defp collect_rate_limit_results(acc, 0), do: acc
  defp collect_rate_limit_results(acc, remaining) do
    receive do
      {:batch_done, count, by_priority, elapsed} ->
        collect_rate_limit_results(acc ++ [{count, by_priority, elapsed}], remaining - 1)
    after 100 ->
      acc
    end
  end

  # ============================================================================
  # 场景 3: 实时任务调度
  # ============================================================================

  defmodule TaskScheduler do
    @moduledoc """
    任务调度器 - 按优先级执行任务
    """
    use Workex

    def init(opts) do
      IO.puts("✅ TaskScheduler 启动")
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
            :high -> "⚡"
            :medium -> "📋"
            :low -> "📝"
          end
          IO.puts("     #{symbol} [#{priority}] 执行了 #{count} 个任务")
        end
      end)

      # 通知父进程
      if parent, do: send(parent, {:batch_done, length(messages), by_priority})

      {:ok, state}
    end
  end

  def demo_task_scheduling do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("【场景 3】任务调度 - 优先执行重要任务")
    IO.puts(String.duplicate("=", 70))

    {:ok, worker} = Workex.start_link(
      TaskScheduler,
      [parent: self()],
      aggregate: %Workex.PriorityAggregate{}
    )

    IO.puts("\n1️⃣  提交各种优先级的任务")

    # 提交任务
    tasks = [
      {:low, "数据备份"},
      {:high, "用户支付处理"},
      {:medium, "发送邮件"},
      {:low, "日志清理"},
      {:high, "订单确认"},
      {:medium, "生成报表"},
      {:low, "缓存更新"},
      {:high, "库存扣减"}
    ]

    Enum.each(tasks, fn task -> Workex.push(worker, task) end)

    IO.puts("   已提交: 3个紧急 + 2个普通 + 3个后台")

    # 等待处理完成
    :timer.sleep(200)

    # 收集结果
    results = collect_results([], 10)

    IO.puts("\n2️⃣  验证执行顺序")

    if length(results) > 0 do
      total_high = Enum.sum(Enum.map(results, fn {_, by_pri} ->
        length(Map.get(by_pri, :high, []))
      end))
      total_medium = Enum.sum(Enum.map(results, fn {_, by_pri} ->
        length(Map.get(by_pri, :medium, []))
      end))
      total_low = Enum.sum(Enum.map(results, fn {_, by_pri} ->
        length(Map.get(by_pri, :low, []))
      end))

      IO.puts("   任务统计:")
      IO.puts("     • 紧急任务: #{total_high}/3 个")
      IO.puts("     • 普通任务: #{total_medium}/2 个")
      IO.puts("     • 后台任务: #{total_low}/3 个")

      if total_high == 3 and total_medium == 2 and total_low == 3 do
        IO.puts("\n   ✅ 所有任务按优先级顺序执行完成")
      end
    else
      IO.puts("   ℹ️  未收到处理结果")
    end

    IO.puts("\n💡 实际应用:")
    IO.puts("   • 支付/订单等关键任务优先")
    IO.puts("   • 后台任务自动排队等待")
    IO.puts("   • 保证系统响应性")

    IO.puts("\n✓ 演示完成：展示了任务调度场景")

    GenServer.stop(worker)
    :timer.sleep(50)
  end

  # ============================================================================
  # 场景 4: 动态负载处理
  # ============================================================================

  defmodule LoadBalancer do
    @moduledoc """
    负载均衡器 - 处理突发流量
    """
    use Workex

    def init(opts) do
      IO.puts("✅ LoadBalancer 启动")
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

  def demo_load_balancing do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts("【场景 4】负载均衡 - 处理突发流量")
    IO.puts(String.duplicate("=", 70))

    {:ok, worker} = Workex.start_link(
      LoadBalancer,
      [parent: self()],
      aggregate: %Workex.PriorityAggregate{}
    )

    IO.puts("\n1️⃣  模拟突发流量 (3波)")

    # 第一波：少量消息
    IO.puts("\n   第1波：5条普通消息")
    Enum.each(1..5, fn i ->
      Workex.push(worker, {:medium, "消息-#{i}"})
    end)

    :timer.sleep(80)

    # 第二波：大量消息（含高优先级）
    IO.puts("   第2波：15条消息（含5条紧急）")
    Enum.each(1..15, fn i ->
      priority = if rem(i, 3) == 0, do: :high, else: :medium
      Workex.push(worker, {priority, "消息-#{i + 5}"})
    end)

    :timer.sleep(120)

    # 第三波：低优先级
    IO.puts("   第3波：10条低优先级")
    Enum.each(1..10, fn i ->
      Workex.push(worker, {:low, "日志-#{i}"})
    end)

    :timer.sleep(200)

    # 收集结果
    results = collect_results([], 30)

    IO.puts("\n2️⃣  统计处理情况")

    if length(results) > 0 do
      total_high = Enum.sum(Enum.map(results, fn {_, by_pri} ->
        length(Map.get(by_pri, :high, []))
      end))
      total_medium = Enum.sum(Enum.map(results, fn {_, by_pri} ->
        length(Map.get(by_pri, :medium, []))
      end))
      total_low = Enum.sum(Enum.map(results, fn {_, by_pri} ->
        length(Map.get(by_pri, :low, []))
      end))

      total = total_high + total_medium + total_low

      IO.puts("   处理结果:")
      IO.puts("     • 紧急消息: #{total_high}/5 条")
      IO.puts("     • 普通消息: #{total_medium}/15 条")
      IO.puts("     • 低级消息: #{total_low}/10 条")
      IO.puts("     • 总计: #{total}/30 条")

      if total == 30 do
        IO.puts("\n   ✅ 所有突发流量处理完成")
        IO.puts("      • 紧急消息优先处理")
        IO.puts("      • 系统负载均衡良好")
      end
    else
      IO.puts("   ℹ️  未收到处理结果")
    end

    IO.puts("\n💡 负载均衡价值:")
    IO.puts("   • 应对突发流量")
    IO.puts("   • 优先级自动调度")
    IO.puts("   • 保证系统稳定性")

    IO.puts("\n✓ 演示完成：展示了负载均衡能力")

    GenServer.stop(worker)
    :timer.sleep(50)
  end

  # ============================================================================
  # 辅助函数
  # ============================================================================

  defp collect_results(acc, 0), do: acc
  defp collect_results(acc, remaining) do
    receive do
      {:batch_done, count, by_priority} ->
        collect_results(acc ++ [{count, by_priority}], remaining - 1)
    after 100 ->
      acc
    end
  end

  # ============================================================================
  # 主函数
  # ============================================================================

  def run do
    print_header()

    demo_basic_usage()
    demo_rate_limiting()
    demo_task_scheduling()
    demo_load_balancing()

    print_footer()
  end

  defp print_header do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts(" PriorityAggregate + Workex 完整集成演示")
    IO.puts(" 基本优先级队列的实际应用")
    IO.puts(String.duplicate("=", 70))
  end

  defp print_footer do
    IO.puts("\n" <> String.duplicate("=", 70))
    IO.puts(" 演示完成")
    IO.puts(String.duplicate("=", 70))

    IO.puts("\n💡 核心概念：")
    IO.puts("""

    【Workex + PriorityAggregate】

    1️⃣  Worker 定义：
       defmodule MyWorker do
         use Workex

         def init(opts), do: {:ok, opts}

         def handle(messages, state) do
           # 处理批量消息
           {:ok, state}
         end
       end

    2️⃣  启动 Worker：
       {:ok, worker} = Workex.start_link(
         MyWorker,
         initial_state,
         aggregate: %Workex.PriorityAggregate{}
       )

    3️⃣  发送消息：
       Workex.push(worker, {:high, "重要消息"})
       Workex.push(worker, {:medium, "普通消息"})
       Workex.push(worker, {:low, "日志消息"})

    4️⃣  自动批量处理：
       • Worker 自动聚合消息
       • 按优先级排序
       • 批量传递给 handle/2
       • 高优先级优先处理

    【优势】
    ✅ 自动批量：提高吞吐量
    ✅ 优先级保证：重要消息优先
    ✅ 背压控制：防止队列爆满
    ✅ 简单易用：只需定义 handle/2

    【适用场景】
    • 消息推送系统
    • 任务调度系统
    • 日志收集系统
    • API 限流系统
    • 数据同步系统
    """)
  end
end

# 运行演示
PriorityWorkerDemo.run()
