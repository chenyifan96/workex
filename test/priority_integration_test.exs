defmodule PriorityIntegrationTest do
  use ExUnit.Case

  @moduletag :integration

  describe "优先级队列与 Workex 框架集成测试" do

    defmodule TestWorker do
      use Workex

      def init(test_pid), do: {:ok, test_pid}

      def handle(messages, test_pid) do
        # 如果消息中包含 :delay，延迟处理以让 worker 保持忙碌
        if Enum.any?(messages, fn {_, msg} -> msg == :delay or msg == :init end) do
          :timer.sleep(100)
        end

        send(test_pid, {:processed, messages})
        {:ok, test_pid}
      end
    end

    test "基本功能：消息按优先级排序处理" do
      {:ok, pid} = Workex.start_link(
        TestWorker,
        self(),
        aggregate: %Workex.PriorityAggregate{}
      )

      # 发送不同优先级的消息
      Workex.push(pid, {:low, "低优先级-1"})
      Workex.push(pid, {:high, "高优先级-1"})
      Workex.push(pid, {:medium, "中优先级-1"})
      Workex.push(pid, {:low, "低优先级-2"})

      # 第一条立即处理
      assert_receive {:processed, [{:low, "低优先级-1"}]}, 500

      # 其他消息按优先级排序处理
      assert_receive {:processed, messages}, 500
      assert messages == [
        {:high, "高优先级-1"},
        {:medium, "中优先级-1"},
        {:low, "低优先级-2"}
      ]
    end

    test "限流功能：队列满时拒绝新消息（replace_oldest: false）" do
      {:ok, pid} = Workex.start_link(
        TestWorker,
        self(),
        aggregate: %Workex.PriorityAggregate{},
        max_size: 5,
        replace_oldest: false
      )

      # 发送一条慢消息让 Worker 忙碌
      Workex.push(pid, {:high, :delay})
      :timer.sleep(20)  # 确保 worker 开始处理延迟消息

      # 快速填充队列到最大容量（在 worker 处理完延迟消息之前）
      for i <- 1..5 do
        assert :ok == Workex.push_ack(pid, {:low, "消息-#{i}"})
      end

      # 队列满后应该拒绝新消息
      assert {:error, :max_capacity} == Workex.push_ack(pid, {:low, "应被拒绝"})

      # 清空消息
      assert_receive {:processed, [{:high, :delay}]}, 1000
      assert_receive {:processed, _}, 1000
    end

    test "智能挤出：高优先级挤出低优先级" do
      {:ok, pid} = Workex.start_link(
        TestWorker,
        self(),
        aggregate: %Workex.PriorityAggregate{},
        max_size: 10,
        replace_oldest: true
      )

      # 让 Worker 忙碌
      Workex.push(pid, {:high, :init})
      :timer.sleep(10)

      # 填充 5 条低优先级
      for i <- 1..5 do
        assert :ok == Workex.push_ack(pid, {:low, "低-#{i}"})
      end

      # 填充 5 条中优先级（队列满：10条）
      for i <- 1..5 do
        assert :ok == Workex.push_ack(pid, {:medium, "中-#{i}"})
      end

      # 添加 3 条高优先级（应该挤出 3 条低优先级）
      for i <- 1..3 do
        assert :ok == Workex.push_ack(pid, {:high, "高-#{i}"})
      end

      # 等待处理：收集所有处理的消息
      assert_receive {:processed, [{:high, :init}]}, 1000

      # 收集所有处理的消息（可能被分批处理）
      all_messages = collect_all_processed(10, [])

      # 验证结果
      high_count = Enum.count(all_messages, fn {p, _} -> p == :high end)
      medium_count = Enum.count(all_messages, fn {p, _} -> p == :medium end)
      low_count = Enum.count(all_messages, fn {p, _} -> p == :low end)

      assert high_count == 3, "应该有 3 条高优先级"
      assert medium_count == 5, "应该保留 5 条中优先级"
      assert low_count == 2, "应该只剩 2 条低优先级（3条被挤出）"
      assert length(all_messages) == 10, "总共应该是 10 条消息"

      # 验证顺序：高 -> 中 -> 低
      {high_msgs, rest1} = Enum.split_while(all_messages, fn {p, _} -> p == :high end)
      {medium_msgs, low_msgs} = Enum.split_while(rest1, fn {p, _} -> p == :medium end)

      assert length(high_msgs) == 3
      assert length(medium_msgs) == 5
      assert length(low_msgs) == 2

      # 验证被挤出的是最老的低优先级（低-1, 低-2, 低-3）
      remaining_low = Enum.map(low_msgs, fn {:low, msg} -> msg end)
      assert remaining_low == ["低-4", "低-5"]
    end

    test "智能挤出：高优先级挤出中优先级（无低优先级时）" do
      {:ok, pid} = Workex.start_link(
        TestWorker,
        self(),
        aggregate: %Workex.PriorityAggregate{},
        max_size: 10,
        replace_oldest: true
      )

      # 让 Worker 忙碌
      Workex.push(pid, {:high, :init})
      :timer.sleep(10)

      # 只填充中优先级（10条）
      for i <- 1..10 do
        assert :ok == Workex.push_ack(pid, {:medium, "中-#{i}"})
      end

      # 添加 3 条高优先级（应该挤出 3 条中优先级）
      for i <- 1..3 do
        assert :ok == Workex.push_ack(pid, {:high, "高-#{i}"})
      end

      # 等待处理：收集所有处理的消息
      assert_receive {:processed, [{:high, :init}]}, 1000

      # 收集所有处理的消息（可能被分批处理）
      all_messages = collect_all_processed(10, [])

      # 验证结果
      high_count = Enum.count(all_messages, fn {p, _} -> p == :high end)
      medium_count = Enum.count(all_messages, fn {p, _} -> p == :medium end)
      low_count = Enum.count(all_messages, fn {p, _} -> p == :low end)

      assert high_count == 3, "应该有 3 条高优先级"
      assert medium_count == 7, "应该只剩 7 条中优先级（3条被挤出）"
      assert low_count == 0, "不应该有低优先级"
      assert length(all_messages) == 10, "总共应该是 10 条消息"

      # 验证被挤出的是最老的中优先级（中-1, 中-2, 中-3）
      remaining_medium = all_messages
      |> Enum.filter(fn {p, _} -> p == :medium end)
      |> Enum.map(fn {:medium, msg} -> msg end)

      assert remaining_medium == ["中-4", "中-5", "中-6", "中-7", "中-8", "中-9", "中-10"]
    end

    test "实际场景：告警系统" do
      {:ok, pid} = Workex.start_link(
        TestWorker,
        self(),
        aggregate: %Workex.PriorityAggregate{},
        max_size: 20
      )

      # 模拟各种级别的告警
      alerts = [
        {:info, "系统启动"},
        {:warning, "内存使用 70%"},
        {:critical, "数据库连接失败！"},
        {:info, "用户登录"},
        {:critical, "磁盘空间不足！"},
        {:warning, "API 响应慢"}
      ]

      # 转换为优先级并发送
      Enum.each(alerts, fn {level, msg} ->
        priority = case level do
          :critical -> :high
          :warning -> :medium
          :info -> :low
        end
        Workex.push(pid, {priority, {level, msg}})
      end)

      # 第一条立即处理
      assert_receive {:processed, [{:low, {:info, "系统启动"}}]}, 500

      # 其他消息按优先级处理
      assert_receive {:processed, messages}, 500

      # 验证 critical 告警优先处理
      [first | _] = messages
      {priority, {level, _}} = first
      assert priority == :high
      assert level == :critical

      # 验证处理顺序：critical -> warning -> info
      priorities = Enum.map(messages, fn {p, {level, _}} -> {p, level} end)

      # 所有 critical 应该在前面
      critical_count = Enum.count(priorities, fn {_, level} -> level == :critical end)
      first_n = Enum.take(priorities, critical_count)
      assert Enum.all?(first_n, fn {_, level} -> level == :critical end)
    end

    test "并发场景：多个生产者同时推送" do
      {:ok, pid} = Workex.start_link(
        TestWorker,
        self(),
        aggregate: %Workex.PriorityAggregate{},
        max_size: 50
      )

      # 让 Worker 忙碌一会儿
      Workex.push(pid, {:high, :init})
      :timer.sleep(10)

      # 启动多个并发生产者
      tasks = for i <- 1..10 do
        Task.async(fn ->
          priority = case rem(i, 3) do
            0 -> :high
            1 -> :medium
            2 -> :low
          end

          for j <- 1..5 do
            Workex.push(pid, {priority, "生产者#{i}-消息#{j}"})
          end
        end)
      end

      # 等待所有生产者完成
      Enum.each(tasks, &Task.await/1)

      # 等待处理：收集所有处理的消息
      assert_receive {:processed, [{:high, :init}]}, 1000

      # 收集所有处理的消息（可能被分批处理）
      all_messages = collect_all_processed(50, [])

      # 验证所有消息都被处理（50条）
      assert length(all_messages) == 50

      # 验证消息按优先级排序
      {high, rest} = Enum.split_while(all_messages, fn {p, _} -> p == :high end)
      {medium, low} = Enum.split_while(rest, fn {p, _} -> p == :medium end)

      # 验证每个优先级的数量
      assert length(high) > 0
      assert length(medium) > 0
      assert length(low) > 0
      assert length(high) + length(medium) + length(low) == 50
    end

    test "边界情况：空队列不会崩溃" do
      {:ok, pid} = Workex.start_link(
        TestWorker,
        self(),
        aggregate: %Workex.PriorityAggregate{}
      )

      # 不发送任何消息，Worker 应该保持空闲
      :timer.sleep(100)

      # 发送一条消息确认系统仍在运行
      Workex.push(pid, {:high, "测试消息"})
      assert_receive {:processed, [{:high, "测试消息"}]}, 500
    end

    test "错误处理：无效的消息格式" do
      aggregate = %Workex.PriorityAggregate{}

      # 测试无效的优先级
      assert {:error, :invalid_message_format} ==
        Workex.PriorityAggregate.add(aggregate, {:invalid, "消息"})

      # 测试错误的消息格式
      assert {:error, :invalid_message_format} ==
        Workex.PriorityAggregate.add(aggregate, "非元组消息")
    end
  end

  # 辅助函数：收集所有处理的消息
  defp collect_all_processed(expected_count, acc) when length(acc) >= expected_count do
    acc
  end

  defp collect_all_processed(expected_count, acc) do
    receive do
      {:processed, messages} ->
        collect_all_processed(expected_count, acc ++ messages)
    after
      500 ->
        acc
    end
  end
end
