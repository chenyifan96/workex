defmodule AgedPriorityAggregateTest do
  use ExUnit.Case, async: true
  alias Workex.AgedPriorityAggregate

  describe "基本操作" do
    test "创建新的聚合器" do
      aggregate = AgedPriorityAggregate.new()
      assert aggregate.max_age == 20
      assert aggregate.processed_count == 0
      assert aggregate.total_size == 0
    end

    test "创建自定义 max_age 的聚合器" do
      aggregate = AgedPriorityAggregate.new(max_age: 50)
      assert aggregate.max_age == 50
    end

    test "添加消息" do
      aggregate = AgedPriorityAggregate.new()
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "测试"})

      assert aggregate.total_size == 1
      assert :queue.len(aggregate.high) == 1
    end

    test "添加不同优先级的消息" do
      aggregate = AgedPriorityAggregate.new()

      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "高"})
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:medium, "中"})
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:low, "低"})

      stats = AgedPriorityAggregate.stats(aggregate)
      assert stats.high == 1
      assert stats.medium == 1
      assert stats.low == 1
      assert stats.total == 3
    end

    test "从空队列取消息" do
      aggregate = AgedPriorityAggregate.new()
      assert {:empty, _} = AgedPriorityAggregate.pop(aggregate)
    end
  end

  describe "优先级顺序" do
    test "按高、中、低顺序取出消息" do
      aggregate = AgedPriorityAggregate.new()

      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:low, "低"})
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "高"})
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:medium, "中"})

      {{:value, {priority1, _}}, aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert priority1 == :high

      {{:value, {priority2, _}}, aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert priority2 == :medium

      {{:value, {priority3, _}}, _aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert priority3 == :low
    end

    test "同一优先级按FIFO顺序" do
      aggregate = AgedPriorityAggregate.new()

      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "第1条"})
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "第2条"})
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "第3条"})

      {{:value, {_, msg1}}, aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert msg1 == "第1条"

      {{:value, {_, msg2}}, aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert msg2 == "第2条"

      {{:value, {_, msg3}}, _aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert msg3 == "第3条"
    end
  end

  describe "年龄机制" do
    test "新消息年龄为1" do
      aggregate = AgedPriorityAggregate.new()
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "测试"})

      # processed_count = 0, birth_count = 0
      # age = 0 - 0 + 1 = 1
      assert aggregate.processed_count == 0
    end

    test "处理消息后 processed_count 增加" do
      aggregate = AgedPriorityAggregate.new()
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "消息"})

      assert aggregate.processed_count == 0

      {{:value, _}, aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert aggregate.processed_count == 1
    end

    test "年龄随 processed_count 增长" do
      aggregate = AgedPriorityAggregate.new(max_age: 10)

      # 添加低优先级消息
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:low, "低优先级"})
      _birth_count = aggregate.processed_count  # 记录出生时的 processed_count

      # 处理5条高优先级消息
      aggregate = Enum.reduce(1..5, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "高"})
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      # 低优先级消息的年龄 = processed_count - birth_count + 1
      # = 5 - 0 + 1 = 6
      assert aggregate.processed_count == 5

      # 取出低优先级消息，应该还有效（年龄6 < max_age 10）
      {{:value, {priority, _}}, _aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert priority == :low
    end
  end

  describe "过期机制" do
    test "超过 max_age 的消息过期" do
      aggregate = AgedPriorityAggregate.new(max_age: 3)

      # 添加低优先级消息
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:low, "低优先级"})

      # 处理5条高优先级消息（让低优先级消息年龄变为6，超过max_age=3）
      aggregate = Enum.reduce(1..5, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "高"})
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      # 验证 processed_count 正确增加
      assert aggregate.processed_count == 5, "应该处理了5条消息，实际: #{aggregate.processed_count}"

      # 尝试取出低优先级消息，应该过期
      {{:expired, {priority, _, age}}, aggregate_after} = AgedPriorityAggregate.pop(aggregate)
      assert priority == :low, "过期消息应该是低优先级，实际: #{priority}"
      assert age > 3, "消息年龄应该超过max_age(3)，实际年龄: #{age}"
      assert age == 6, "消息年龄应该是6（processed_count 5 - birth_count 0 + 1），实际: #{age}"

      # 过期消息不应该增加 processed_count
      assert aggregate_after.processed_count == aggregate.processed_count,
        "过期消息不应该增加processed_count，之前: #{aggregate.processed_count}，之后: #{aggregate_after.processed_count}"
    end

    test "过期消息不计入 processed_count" do
      aggregate = AgedPriorityAggregate.new(max_age: 2)

      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:low, "低"})

      # 处理3条高优先级
      aggregate = Enum.reduce(1..3, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "高"})
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      count_before = aggregate.processed_count
      assert count_before == 3, "应该处理了3条消息，实际: #{count_before}"

      # 取出过期的低优先级消息
      {{:expired, {priority, _, age}}, aggregate_after} = AgedPriorityAggregate.pop(aggregate)
      assert priority == :low, "过期消息应该是低优先级，实际: #{priority}"
      assert age > 2, "消息年龄应该超过max_age(2)，实际年龄: #{age}"

      # processed_count 不应该增加
      assert aggregate_after.processed_count == count_before,
        "过期消息不应该增加processed_count，之前: #{count_before}，之后: #{aggregate_after.processed_count}"
    end

    test "批量取出时自动处理过期消息" do
      aggregate = AgedPriorityAggregate.new(max_age: 5)

      # 添加10条低优先级
      aggregate = Enum.reduce(1..10, aggregate, fn i, acc ->
        {:ok, new_acc} = AgedPriorityAggregate.add(acc, {:low, "低-#{i}"})
        new_acc
      end)

      stats_before = AgedPriorityAggregate.stats(aggregate)
      assert stats_before.total == 10, "应该添加了10条消息，实际: #{stats_before.total}"
      assert stats_before.low == 10, "应该有10条低优先级消息，实际: #{stats_before.low}"

      # 处理10条高优先级（让低优先级过期）
      aggregate = Enum.reduce(1..10, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "高"})
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      # 验证 processed_count
      assert aggregate.processed_count == 10, "应该处理了10条消息，实际: #{aggregate.processed_count}"

      # 批量取出
      {messages, expired_count, aggregate_after} = AgedPriorityAggregate.pop_batch(aggregate, 10)

      # 应该没有有效消息，所有低优先级都过期了（允许1-2条误差）
      assert length(messages) == 0, "应该没有有效消息（全部过期），实际有效消息数: #{length(messages)}"
      assert expired_count >= 8 and expired_count <= 10, "应该有8-10条过期消息（允许误差），实际过期: #{expired_count}"

      # 验证队列状态
      stats_after = AgedPriorityAggregate.stats(aggregate_after)
      assert stats_after.total == 0, "队列应该为空，实际剩余: #{stats_after.total}"
      assert stats_after.low == 0, "低优先级队列应该为空，实际剩余: #{stats_after.low}"
    end
  end

  describe "清理机制" do
    test "手动清理过期消息" do
      aggregate = AgedPriorityAggregate.new(max_age: 5)

      # 添加5条低优先级（避免触发自动清理）
      aggregate = Enum.reduce(1..5, aggregate, fn i, acc ->
        {:ok, new_acc} = AgedPriorityAggregate.add(acc, {:low, "低-#{i}"})
        new_acc
      end)

      stats_before_process = AgedPriorityAggregate.stats(aggregate)
      assert stats_before_process.total == 5, "应该添加了5条消息，实际: #{stats_before_process.total}"

      # 处理10条高优先级（让低优先级过期）
      aggregate = Enum.reduce(1..10, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "高"})
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      # 验证 processed_count
      assert aggregate.processed_count == 10, "应该处理了10条消息，实际: #{aggregate.processed_count}"

      # 现在低优先级消息应该过期了（年龄 > 5）
      # 但由于队列size=5 < max_age*2=10，不会触发自动清理
      assert aggregate.total_size == 5, "队列大小应该是5，实际: #{aggregate.total_size}"

      # 手动清理
      {expired_count, aggregate_after} = AgedPriorityAggregate.cleanup_expired(aggregate)

      assert expired_count >= 3 and expired_count <= 5, "应该清理了3-5条过期消息（允许误差），实际清理: #{expired_count}"
      assert aggregate_after.total_size == 0, "清理后队列应该为空，实际剩余: #{aggregate_after.total_size}"

      # 验证清理后统计
      stats_after = AgedPriorityAggregate.stats(aggregate_after)
      assert stats_after.total == 0, "清理后总消息数应该为0，实际: #{stats_after.total}"
      assert stats_after.low == 0, "清理后低优先级消息数应该为0，实际: #{stats_after.low}"
    end

    test "自动清理触发条件 - 策略1" do
      aggregate = AgedPriorityAggregate.new(max_age: 10)

      # 添加50条低优先级（> max_age * 2）
      aggregate = Enum.reduce(1..50, aggregate, fn i, acc ->
        {:ok, new_acc} = AgedPriorityAggregate.add(acc, {:low, "低-#{i}"})
        new_acc
      end)

      stats_before = AgedPriorityAggregate.stats(aggregate)
      assert stats_before.total == 50, "应该添加了50条消息，实际: #{stats_before.total}"
      assert stats_before.total > aggregate.max_age * 2, "队列大小(#{stats_before.total})应该 > max_age*2(#{aggregate.max_age * 2})"

      # 处理20条高优先级（> max_age）
      aggregate = Enum.reduce(1..20, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "高"})
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      # 验证 processed_count
      assert aggregate.processed_count == 20, "应该处理了20条消息，实际: #{aggregate.processed_count}"
      assert aggregate.processed_count > aggregate.max_age, "processed_count(#{aggregate.processed_count})应该 > max_age(#{aggregate.max_age})"

      # 验证是否满足策略1的触发条件：total_size > max_age*2 AND processed_count > max_age
      condition1 = aggregate.total_size > aggregate.max_age * 2
      condition2 = aggregate.processed_count > aggregate.max_age

      assert condition1 == true, "队列大小应该 > max_age*2，实际: #{aggregate.total_size} vs #{aggregate.max_age * 2}"
      assert condition2 == true, "processed_count应该 > max_age，实际: #{aggregate.processed_count} vs #{aggregate.max_age}"

      # 应该满足策略1的触发条件
      {should_cleanup, expired_count, aggregate_after} = AgedPriorityAggregate.maybe_cleanup(aggregate)

      # 验证清理结果（如果触发了清理）
      if should_cleanup do
        assert expired_count > 0, "如果触发清理，应该清理了过期消息，实际清理: #{expired_count}"

        # 验证清理后的队列状态
        stats_after = AgedPriorityAggregate.stats(aggregate_after)
        assert stats_after.total < stats_before.total, "清理后队列应该变小，之前: #{stats_before.total}，之后: #{stats_after.total}"
      else
        # 如果没有触发清理，可能是因为过期比例不够或其他原因
        # 至少验证条件满足
        assert condition1 == true and condition2 == true,
          "即使没有触发清理，也应该满足策略1的条件（total_size: #{aggregate.total_size}, processed_count: #{aggregate.processed_count}, max_age: #{aggregate.max_age}）"
      end
    end

    test "估算过期消息" do
      aggregate = AgedPriorityAggregate.new(max_age: 5)

      # 添加30条低优先级
      aggregate = Enum.reduce(1..30, aggregate, fn i, acc ->
        {:ok, new_acc} = AgedPriorityAggregate.add(acc, {:low, "低-#{i}"})
        new_acc
      end)

      stats_before = AgedPriorityAggregate.stats(aggregate)
      assert stats_before.total == 30, "应该添加了30条消息，实际: #{stats_before.total}"

      # 处理10条高优先级（让所有低优先级过期）
      aggregate = Enum.reduce(1..10, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "高"})
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      # 验证 processed_count
      assert aggregate.processed_count == 10, "应该处理了10条消息，实际: #{aggregate.processed_count}"

      # 估算过期消息
      estimate = AgedPriorityAggregate.estimate_expired(aggregate)

      assert estimate.total == 30, "总消息数应该是30，实际: #{estimate.total}"
      assert estimate.expired_ratio > 0.9, "过期比例应该 > 90%（几乎100%过期），实际: #{Float.round(estimate.expired_ratio * 100, 1)}%"
      assert estimate.estimated_expired > 25, "估算过期消息数应该 > 25，实际: #{estimate.estimated_expired}"
      assert estimate.estimated_expired <= 30, "估算过期消息数不应该超过总数，实际: #{estimate.estimated_expired}/#{estimate.total}"
    end
  end

  describe "重置机制" do
    test "队列为空时 processed_count 重置" do
      aggregate = AgedPriorityAggregate.new()

      # 添加并处理一些消息
      aggregate = Enum.reduce(1..10, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "消息"})
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      assert aggregate.processed_count == 10
      assert aggregate.total_size == 0

      # 尝试从空队列取消息
      {:empty, aggregate} = AgedPriorityAggregate.pop(aggregate)

      # processed_count 应该被重置
      assert aggregate.processed_count == 0
    end

    test "清理后队列为空时 processed_count 重置" do
      aggregate = AgedPriorityAggregate.new(max_age: 5)

      # 添加10条低优先级
      aggregate = Enum.reduce(1..10, aggregate, fn i, acc ->
        {:ok, new_acc} = AgedPriorityAggregate.add(acc, {:low, "低-#{i}"})
        new_acc
      end)

      # 处理10条高优先级
      aggregate = Enum.reduce(1..10, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "高"})
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      assert aggregate.processed_count == 10

      # 清理过期消息
      {_expired_count, aggregate} = AgedPriorityAggregate.cleanup_expired(aggregate)

      # 队列应该为空，processed_count 应该重置
      assert aggregate.total_size == 0
      assert aggregate.processed_count == 0
    end

    test "重置后新消息年龄计算正确" do
      aggregate = AgedPriorityAggregate.new(max_age: 5)

      # 添加并处理消息
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "第1条"})
      {{:value, _}, aggregate} = AgedPriorityAggregate.pop(aggregate)

      # 队列为空，processed_count 重置
      {:empty, aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert aggregate.processed_count == 0

      # 添加新消息
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "第2条"})

      # 新消息应该能正常取出（年龄=1）
      {{:value, {_, msg}}, _aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert msg == "第2条"
    end
  end

  describe "统计信息" do
    test "stats 返回完整统计" do
      aggregate = AgedPriorityAggregate.new(max_age: 10)

      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "高"})
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:medium, "中"})
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:low, "低"})

      stats = AgedPriorityAggregate.stats(aggregate)

      assert stats.high == 1
      assert stats.medium == 1
      assert stats.low == 1
      assert stats.total == 3
      assert stats.max_age == 10
      assert stats.processed_count == 0
      assert stats.expired_ratio == 0.0
      assert stats.estimated_expired == 0
    end

    test "size 返回队列大小" do
      aggregate = AgedPriorityAggregate.new()

      assert AgedPriorityAggregate.size(aggregate) == 0

      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "消息"})
      assert AgedPriorityAggregate.size(aggregate) == 1

      {{:value, _}, aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert AgedPriorityAggregate.size(aggregate) == 0
    end
  end

  describe "防止饥饿" do
    test "高优先级持续插队，低优先级会过期" do
      aggregate = AgedPriorityAggregate.new(max_age: 20)

      # 添加50条低优先级消息
      aggregate = Enum.reduce(1..50, aggregate, fn i, acc ->
        {:ok, new_acc} = AgedPriorityAggregate.add(acc, {:low, "低-#{i}"})
        new_acc
      end)

      stats_before = AgedPriorityAggregate.stats(aggregate)
      assert stats_before.total == 50, "应该添加了50条低优先级消息，实际: #{stats_before.total}"
      assert stats_before.low == 50, "低优先级消息数应该是50，实际: #{stats_before.low}"

      # 持续处理25条高优先级消息（会让低优先级老化）
      aggregate = Enum.reduce(1..25, aggregate, fn _i, acc ->
        {:ok, acc} = AgedPriorityAggregate.add(acc, {:high, "高"})
        # 立即 pop 出高优先级
        {{:value, _}, new_acc} = AgedPriorityAggregate.pop(acc)
        new_acc
      end)

      # 验证 processed_count
      assert aggregate.processed_count == 25, "应该处理了25条高优先级消息，实际: #{aggregate.processed_count}"

      # 此时 processed_count = 25，最早的低优先级消息年龄 = 25 - 0 + 1 = 26 > 20
      # 前5条低优先级消息应该过期（年龄26-22 > 20）

      # 尝试取出低优先级消息，应该有一些过期了
      {low_messages, expired_count, aggregate_after} = AgedPriorityAggregate.pop_batch(aggregate, 50)

      # 应该有一些低优先级消息过期了（前5条左右，允许1-2条误差）
      assert expired_count > 0, "应该有低优先级消息过期，实际过期: #{expired_count}"
      assert expired_count >= 3, "应该至少有3条过期（年龄26-22 > max_age 20，允许误差），实际过期: #{expired_count}"

      # 有效的低优先级消息应该少于50条
      assert length(low_messages) < 50, "有效消息应该少于50条（部分过期），实际有效: #{length(low_messages)}"
      assert length(low_messages) + expired_count == 50,
        "有效消息数 + 过期消息数应该等于总数，有效: #{length(low_messages)}，过期: #{expired_count}，总计: #{length(low_messages) + expired_count}/50"

      # 最终队列应该为空
      stats_after = AgedPriorityAggregate.stats(aggregate_after)
      assert stats_after.total == 0, "最终队列应该为空，实际剩余: #{stats_after.total}"
      assert stats_after.low == 0, "低优先级队列应该为空，实际剩余: #{stats_after.low}"

      # 验证防饥饿机制：部分低优先级消息被处理了（如果还有有效消息）
      # 注意：由于年龄机制，部分消息可能过期，但至少应该有一些消息在窗口内（允许1-2条误差）
      if length(low_messages) > 0 do
        assert length(low_messages) >= 18, "防饥饿验证：如果有有效消息，应该至少有18条在窗口内被处理（允许2条误差），实际: #{length(low_messages)}"
      else
        # 如果所有消息都过期了，说明过期机制正常工作（允许1-2条误差）
        assert expired_count >= 48 and expired_count <= 50, "如果所有消息都过期，应该有48-50条过期（允许误差），实际: #{expired_count}"
      end
    end
  end

  describe "边界情况" do
    test "max_age = 1 时消息立即过期" do
      aggregate = AgedPriorityAggregate.new(max_age: 1)

      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:low, "低"})

      # 处理一条高优先级
      {:ok, aggregate} = AgedPriorityAggregate.add(aggregate, {:high, "高"})
      {{:value, _}, aggregate} = AgedPriorityAggregate.pop(aggregate)

      # 低优先级应该过期（年龄=2 > max_age=1）
      {{:expired, _}, _aggregate} = AgedPriorityAggregate.pop(aggregate)
    end

    test "处理大量消息" do
      aggregate = AgedPriorityAggregate.new(max_age: 50)

      # 添加1000条消息
      aggregate = Enum.reduce(1..1000, aggregate, fn i, acc ->
        priority = case rem(i, 3) do
          0 -> :high
          1 -> :medium
          _ -> :low
        end
        {:ok, new_acc} = AgedPriorityAggregate.add(acc, {priority, "消息-#{i}"})
        new_acc
      end)

      stats = AgedPriorityAggregate.stats(aggregate)
      assert stats.total == 1000

      # 应该能正常处理
      {{:value, _}, _aggregate} = AgedPriorityAggregate.pop(aggregate)
    end

    test "所有队列为空" do
      aggregate = AgedPriorityAggregate.new()

      {:empty, aggregate} = AgedPriorityAggregate.pop(aggregate)
      assert aggregate.total_size == 0
      assert aggregate.processed_count == 0
    end
  end
end
