defmodule PriorityAggregateTest do
  use ExUnit.Case

  alias Workex.PriorityAggregate

  # 测试用的 Worker
  defmodule PriorityWorker do
    use Workex

    def init(test_pid), do: {:ok, test_pid}

    def handle(messages, test_pid) do
      # 处理延迟消息时真正延迟
      if Enum.any?(messages, fn
        {_, :delay} -> true
        _ -> false
      end) do
        :timer.sleep(50)  # 延迟50ms，确保其他消息能被添加到队列
      end

      send(test_pid, {:processed, messages})
      {:ok, test_pid}
    end
  end

  describe "基本功能测试" do
    test "添加不同优先级的消息" do
      aggregate = %PriorityAggregate{}

      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高优先级"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:medium, "中优先级"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低优先级"})

      assert PriorityAggregate.size(aggregate) == 3

      stats = PriorityAggregate.stats(aggregate)
      assert stats.high == 1
      assert stats.medium == 1
      assert stats.low == 1
      assert stats.total == 3
    end

    test "消息格式错误时返回错误" do
      aggregate = %PriorityAggregate{}

      assert {:error, :invalid_message_format} =
        PriorityAggregate.add(aggregate, "无效格式")

      assert {:error, :invalid_message_format} =
        PriorityAggregate.add(aggregate, {:invalid_priority, "消息"})
    end

    test "按优先级顺序提取消息" do
      aggregate = %PriorityAggregate{}

      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:medium, "中1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低2"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高2"})

      {messages, new_aggregate} = PriorityAggregate.value(aggregate)

      # 验证顺序：所有高优先级 -> 所有中优先级 -> 所有低优先级
      assert messages == [
        {:high, "高1"},
        {:high, "高2"},
        {:medium, "中1"},
        {:low, "低1"},
        {:low, "低2"}
      ]

      # 提取后队列为空
      assert PriorityAggregate.size(new_aggregate) == 0
    end

    test "remove_oldest 删除最老的低优先级消息" do
      aggregate = %PriorityAggregate{}

      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:medium, "中1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低2"})

      aggregate = PriorityAggregate.remove_oldest(aggregate)

      # 应该删除了 {:low, "低1"}
      assert PriorityAggregate.size(aggregate) == 3

      stats = PriorityAggregate.stats(aggregate)
      assert stats.low == 1  # 还剩一个低优先级

      {messages, _} = PriorityAggregate.value(aggregate)
      assert messages == [
        {:high, "高1"},
        {:medium, "中1"},
        {:low, "低2"}  # 低1 被删除了
      ]
    end

    test "remove_oldest 在没有低优先级时删除中优先级" do
      aggregate = %PriorityAggregate{}

      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:medium, "中1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:medium, "中2"})

      aggregate = PriorityAggregate.remove_oldest(aggregate)

      # 应该删除了 {:medium, "中1"}
      assert PriorityAggregate.size(aggregate) == 2

      {messages, _} = PriorityAggregate.value(aggregate)
      assert messages == [
        {:high, "高1"},
        {:medium, "中2"}  # 中1 被删除了
      ]
    end

    test "remove_oldest 在只有高优先级时删除高优先级" do
      aggregate = %PriorityAggregate{}

      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高2"})

      aggregate = PriorityAggregate.remove_oldest(aggregate)

      assert PriorityAggregate.size(aggregate) == 1

      {messages, _} = PriorityAggregate.value(aggregate)
      assert messages == [{:high, "高2"}]  # 高1 被删除了
    end

    test "pop 从空队列返回 empty" do
      aggregate = %PriorityAggregate{}

      assert {:empty, ^aggregate} = PriorityAggregate.pop(aggregate)
    end

    test "pop 按优先级顺序取出消息：高 -> 中 -> 低" do
      aggregate = %PriorityAggregate{}

      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:medium, "中1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低2"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高2"})

      # 第一次 pop：应该取出第一条高优先级
      {{:value, item1}, aggregate} = PriorityAggregate.pop(aggregate)
      assert item1 == {:high, "高1"}
      assert PriorityAggregate.size(aggregate) == 4

      # 第二次 pop：应该取出第二条高优先级
      {{:value, item2}, aggregate} = PriorityAggregate.pop(aggregate)
      assert item2 == {:high, "高2"}
      assert PriorityAggregate.size(aggregate) == 3

      # 第三次 pop：应该取出中优先级
      {{:value, item3}, aggregate} = PriorityAggregate.pop(aggregate)
      assert item3 == {:medium, "中1"}
      assert PriorityAggregate.size(aggregate) == 2

      # 第四次 pop：应该取出第一条低优先级
      {{:value, item4}, aggregate} = PriorityAggregate.pop(aggregate)
      assert item4 == {:low, "低1"}
      assert PriorityAggregate.size(aggregate) == 1

      # 第五次 pop：应该取出第二条低优先级
      {{:value, item5}, aggregate} = PriorityAggregate.pop(aggregate)
      assert item5 == {:low, "低2"}
      assert PriorityAggregate.size(aggregate) == 0

      # 第六次 pop：队列已空
      assert {:empty, _} = PriorityAggregate.pop(aggregate)
    end

    test "pop 优先从高优先级队列取出" do
      aggregate = %PriorityAggregate{}

      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:high, "高2"})

      {{:value, {:high, "高1"}}, aggregate} = PriorityAggregate.pop(aggregate)
      assert PriorityAggregate.size(aggregate) == 1

      stats = PriorityAggregate.stats(aggregate)
      assert stats.high == 1
      assert stats.medium == 0
      assert stats.low == 0
    end

    test "pop 在没有高优先级时从中优先级取出" do
      aggregate = %PriorityAggregate{}

      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:medium, "中1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低1"})

      {{:value, {:medium, "中1"}}, aggregate} = PriorityAggregate.pop(aggregate)
      assert PriorityAggregate.size(aggregate) == 1

      stats = PriorityAggregate.stats(aggregate)
      assert stats.high == 0
      assert stats.medium == 0
      assert stats.low == 1
    end

    test "pop 在只有低优先级时从低优先级取出" do
      aggregate = %PriorityAggregate{}

      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低1"})
      {:ok, aggregate} = PriorityAggregate.add(aggregate, {:low, "低2"})

      {{:value, {:low, "低1"}}, aggregate} = PriorityAggregate.pop(aggregate)
      assert PriorityAggregate.size(aggregate) == 1

      {{:value, {:low, "低2"}}, aggregate} = PriorityAggregate.pop(aggregate)
      assert PriorityAggregate.size(aggregate) == 0
    end
  end

  describe "与 Workex 集成测试" do
    test "基本的消息处理流程" do
      {:ok, server} = Workex.start_link(
        PriorityWorker,
        self(),
        aggregate: %PriorityAggregate{}
      )

      Workex.push(server, {:low, "低优先级消息"})
      Workex.push(server, {:high, "高优先级消息"})
      Workex.push(server, {:medium, "中优先级消息"})

      # 第一条消息立即处理
      assert_receive({:processed, [{:low, "低优先级消息"}]})

      # 剩余消息按优先级处理
      assert_receive({:processed, messages})
      assert messages == [
        {:high, "高优先级消息"},
        {:medium, "中优先级消息"}
      ]
    end

    test "队列限制 20 条，超出时挤出低优先级" do
      {:ok, server} = Workex.start_link(
        PriorityWorker,
        self(),
        aggregate: %PriorityAggregate{},
        max_size: 20,
        replace_oldest: true
      )

      # 发送一条延迟消息，让 Worker 忙碌
      Workex.push(server, {:high, :delay})
      :timer.sleep(10)  # 确保 Worker 开始处理

      # 填充 10 条低优先级
      for i <- 1..10 do
        assert :ok == Workex.push_ack(server, {:low, "低-#{i}"})
      end

      # 填充 10 条中优先级（现在队列满了：20 条）
      for i <- 1..10 do
        assert :ok == Workex.push_ack(server, {:medium, "中-#{i}"})
      end

      # 再发送 5 条高优先级（应该挤出 5 条低优先级）
      for i <- 1..5 do
        assert :ok == Workex.push_ack(server, {:high, "高-#{i}"})
      end

      # 等待处理
      assert_receive({:processed, [{:high, :delay}]}, 1000)
      assert_receive({:processed, messages}, 1000)

      # 验证结果
      high_messages = Enum.filter(messages, fn {p, _} -> p == :high end)
      medium_messages = Enum.filter(messages, fn {p, _} -> p == :medium end)
      low_messages = Enum.filter(messages, fn {p, _} -> p == :low end)

      # 应该有 5 条高优先级
      assert length(high_messages) == 5

      # 应该有 10 条中优先级（没有被挤出）
      assert length(medium_messages) == 10

      # 应该只剩 5 条低优先级（5 条被挤出了）
      assert length(low_messages) == 5

      # 总共 20 条
      assert length(messages) == 20

      # 验证被挤出的是最老的低优先级
      low_ids = Enum.map(low_messages, fn {:low, msg} ->
        msg |> String.split("-") |> List.last() |> String.to_integer()
      end)

      # 剩下的应该是 低-6 到 低-10
      assert low_ids == [6, 7, 8, 9, 10]
    end

    test "没有低优先级时，高优先级挤出中优先级" do
      {:ok, server} = Workex.start_link(
        PriorityWorker,
        self(),
        aggregate: %PriorityAggregate{},
        max_size: 10,
        replace_oldest: true
      )

      # 发送延迟消息
      Workex.push(server, {:high, :delay})
      :timer.sleep(10)

      # 填充 10 条中优先级（队列满）
      for i <- 1..10 do
        assert :ok == Workex.push_ack(server, {:medium, "中-#{i}"})
      end

      # 发送 3 条高优先级（应该挤出 3 条中优先级）
      for i <- 1..3 do
        assert :ok == Workex.push_ack(server, {:high, "高-#{i}"})
      end

      # 等待处理
      assert_receive({:processed, [{:high, :delay}]}, 1000)
      assert_receive({:processed, messages}, 1000)

      high_messages = Enum.filter(messages, fn {p, _} -> p == :high end)
      medium_messages = Enum.filter(messages, fn {p, _} -> p == :medium end)

      assert length(high_messages) == 3
      assert length(medium_messages) == 7  # 3 条被挤出

      # 验证被挤出的是最老的中优先级
      medium_ids = Enum.map(medium_messages, fn {:medium, msg} ->
        msg |> String.split("-") |> List.last() |> String.to_integer()
      end)

      assert medium_ids == [4, 5, 6, 7, 8, 9, 10]
    end

    test "处理顺序：高 -> 中 -> 低" do
      {:ok, server} = Workex.start_link(
        PriorityWorker,
        self(),
        aggregate: %PriorityAggregate{}
      )

      # 第一条立即处理
      Workex.push(server, {:low, "第一条"})
      assert_receive({:processed, [{:low, "第一条"}]})

      # 发送延迟消息阻塞 Worker，确保后续消息都进入队列
      Workex.push(server, {:high, :delay})
      :timer.sleep(10)  # 确保 Worker 开始处理 delay 消息

      # 现在发送多条不同优先级的消息
      Workex.push(server, {:low, "低1"})
      Workex.push(server, {:medium, "中1"})
      Workex.push(server, {:high, "高1"})
      Workex.push(server, {:low, "低2"})
      Workex.push(server, {:medium, "中2"})
      Workex.push(server, {:high, "高2"})

      # 先接收延迟消息的处理结果
      assert_receive({:processed, [{:high, :delay}]}, 1000)

      # 再接收剩余消息的处理结果
      assert_receive({:processed, messages}, 1000)

      # 验证顺序
      assert messages == [
        {:high, "高1"},
        {:high, "高2"},
        {:medium, "中1"},
        {:medium, "中2"},
        {:low, "低1"},
        {:low, "低2"}
      ]
    end
  end

  describe "边界情况测试" do
    test "空队列删除不会崩溃" do
      aggregate = %PriorityAggregate{}

      aggregate = PriorityAggregate.remove_oldest(aggregate)

      assert PriorityAggregate.size(aggregate) == 0
    end

    test "提取空队列返回空列表" do
      aggregate = %PriorityAggregate{}

      {messages, _} = PriorityAggregate.value(aggregate)

      assert messages == []
    end
  end
end
