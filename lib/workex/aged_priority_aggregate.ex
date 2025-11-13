defmodule Workex.AgedPriorityAggregate do
  @moduledoc """
  基于消息年龄的优先级聚合器

  核心机制：
  - 每条消息进入队列时，年龄为 1
  - 每处理一条消息，所有未处理消息的年龄 +1
  - 年龄超过阈值（如 21）的消息会被丢弃

  场景示例：
  - 队列容量：20条
  - 进来高、中、低三种消息各10条
  - 由于高优先级插队，处理了20条高优先级消息
  - 此时中、低优先级的年龄从1增长到21
  - 年龄21 > 阈值20 → 自动过期

  使用示例：

      aggregate = %Workex.AgedPriorityAggregate{
        max_age: 20  # 最多等待20条消息被处理
      }

      {:ok, aggregate} = add(aggregate, {:high, "重要消息"})
      {{:value, {priority, message}}, new_aggregate} = pop(aggregate)
  """

  defstruct high: :queue.new(),
            medium: :queue.new(),
            low: :queue.new(),
            total_size: 0,
            max_age: 20,              # 最大年龄（等同于窗口容量）
            processed_count: 0        # 已处理的消息总数

  @type priority :: :high | :medium | :low
  @type message :: any
  @type age :: pos_integer()
  @type aged_item :: {priority, message, age}
  @type t :: %__MODULE__{
    high: :queue.queue(),
    medium: :queue.queue(),
    low: :queue.queue(),
    total_size: non_neg_integer(),
    max_age: pos_integer(),
    processed_count: non_neg_integer()
  }

  @doc """
  创建新的聚合器
  """
  @spec new(keyword) :: t
  def new(opts \\ []) do
    %__MODULE__{
      max_age: Keyword.get(opts, :max_age, 20)
    }
  end

  @doc """
  添加消息到队列，初始年龄为 1

  性能优化：
  - 如果队列中过期消息过多（>50% 且总数>100），会自动触发清理
  """
  @spec add(t, {priority, message}) :: {:ok, t} | {:error, term}
  def add(%__MODULE__{processed_count: count} = aggregate, {priority, message})
      when priority in [:high, :medium, :low] do
    # 性能优化技巧：
    # 不直接存储年龄值1，而是存储消息的"出生时间"（birth_count）
    # 这样年龄会随着 processed_count 自动增长，无需遍历队列
    #
    # 初始年龄验证：
    #   age = processed_count - birth_count + 1
    #   age = count - count + 1 = 1 ✓
    aged_item = {priority, message, count}

    new_aggregate = case priority do
      :high ->
        %{aggregate |
          high: :queue.in(aged_item, aggregate.high),
          total_size: aggregate.total_size + 1
        }
      :medium ->
        %{aggregate |
          medium: :queue.in(aged_item, aggregate.medium),
          total_size: aggregate.total_size + 1
        }
      :low ->
        %{aggregate |
          low: :queue.in(aged_item, aggregate.low),
          total_size: aggregate.total_size + 1
        }
    end

    # 自动清理检查：如果过期消息过多，触发清理
    {_cleaned, _count, final_aggregate} = maybe_cleanup(new_aggregate)

    {:ok, final_aggregate}
  end

  def add(_aggregate, _item) do
    {:error, :invalid_message_format}
  end

  @doc """
  按优先级顺序取出消息，检查年龄并丢弃过期消息

  年龄计算：
  - 消息进入时记录 birth_count
  - 当前年龄 = processed_count - birth_count + 1
  - 如果年龄 > max_age，消息过期

  返回值：
  - {{:value, {priority, message}}, new_aggregate} - 成功取出消息
  - {{:expired, {priority, message, age}}, new_aggregate} - 消息已过期被丢弃
  - {:empty, aggregate} - 队列为空

  性能优化：
  - 当队列为空时，processed_count 会被重置为 0
  - 这防止了 processed_count 在长时间运行中无限增长
  - 因为队列为空时没有消息需要计算年龄，重置是安全的
  """
  @spec pop(t) ::
    {{:value, {priority, message}}, t} |
    {{:expired, {priority, message, age}}, t} |
    {:empty, t}
  def pop(%__MODULE__{
    high: high,
    medium: medium,
    low: low,
    total_size: total_size,
    max_age: max_age,
    processed_count: processed_count
  } = aggregate) do

    cond do
      # 尝试从高优先级取
      :queue.len(high) > 0 ->
        {{:value, {priority, message, birth_count}}, new_high} = :queue.out(high)
        age = processed_count - birth_count + 1

        if age > max_age do
          # 消息过期，丢弃并递归尝试下一个
          new_aggregate = %{aggregate | high: new_high, total_size: total_size - 1}
          {{:expired, {priority, message, age}}, new_aggregate}
        else
          # 消息有效，返回并增加 processed_count
          new_aggregate = %{aggregate |
            high: new_high,
            total_size: total_size - 1,
            processed_count: processed_count + 1
          }
          {{:value, {priority, message}}, new_aggregate}
        end

      # 尝试从中优先级取
      :queue.len(medium) > 0 ->
        {{:value, {priority, message, birth_count}}, new_medium} = :queue.out(medium)
        age = processed_count - birth_count + 1

        if age > max_age do
          # 消息过期，丢弃并递归尝试下一个
          new_aggregate = %{aggregate | medium: new_medium, total_size: total_size - 1}
          {{:expired, {priority, message, age}}, new_aggregate}
        else
          # 消息有效，返回并增加 processed_count
          new_aggregate = %{aggregate |
            medium: new_medium,
            total_size: total_size - 1,
            processed_count: processed_count + 1
          }
          {{:value, {priority, message}}, new_aggregate}
        end

      # 尝试从低优先级取
      :queue.len(low) > 0 ->
        {{:value, {priority, message, birth_count}}, new_low} = :queue.out(low)
        age = processed_count - birth_count + 1

        if age > max_age do
          # 消息过期，丢弃并递归尝试下一个
          new_aggregate = %{aggregate | low: new_low, total_size: total_size - 1}
          {{:expired, {priority, message, age}}, new_aggregate}
        else
          # 消息有效，返回并增加 processed_count
          new_aggregate = %{aggregate |
            low: new_low,
            total_size: total_size - 1,
            processed_count: processed_count + 1
          }
          {{:value, {priority, message}}, new_aggregate}
        end

      # 队列为空 - 重置 processed_count 防止无限增长
      true ->
        # 性能优化：队列为空时重置计数器
        # 这样可以防止 processed_count 无限增长
        # 因为队列为空时没有消息，不需要计算年龄
        reset_aggregate = %{aggregate | processed_count: 0}
        {:empty, reset_aggregate}
    end
  end

  @doc """
  批量取出消息，自动处理过期消息
  返回 {valid_messages, expired_count, new_aggregate}
  """
  @spec pop_batch(t, pos_integer()) :: {list({priority, message}), non_neg_integer(), t}
  def pop_batch(aggregate, max_count \\ 20) do
    pop_batch_recursive(aggregate, max_count, [], 0)
  end

  defp pop_batch_recursive(aggregate, 0, acc, expired_count) do
    {Enum.reverse(acc), expired_count, aggregate}
  end

  defp pop_batch_recursive(aggregate, remaining, acc, expired_count) do
    case pop(aggregate) do
      {{:value, item}, new_aggregate} ->
        pop_batch_recursive(new_aggregate, remaining - 1, [item | acc], expired_count)

      {{:expired, _}, new_aggregate} ->
        # 过期消息不计入 remaining，继续取
        pop_batch_recursive(new_aggregate, remaining, acc, expired_count + 1)

      {:empty, aggregate} ->
        {Enum.reverse(acc), expired_count, aggregate}
    end
  end

  @doc """
  返回当前队列中的总消息数
  """
  @spec size(t) :: non_neg_integer()
  def size(%__MODULE__{total_size: total_size}) do
    total_size
  end

  @doc """
  获取统计信息（包含过期消息估算）
  """
  @spec stats(t) :: %{
    high: non_neg_integer(),
    medium: non_neg_integer(),
    low: non_neg_integer(),
    total: non_neg_integer(),
    processed_count: non_neg_integer(),
    max_age: pos_integer(),
    estimated_expired: non_neg_integer(),
    expired_ratio: float()
  }
  def stats(aggregate) do
    %__MODULE__{
      high: high,
      medium: medium,
      low: low,
      total_size: total_size,
      processed_count: processed_count,
      max_age: max_age
    } = aggregate

    expired_stats = estimate_expired(aggregate)

    %{
      high: :queue.len(high),
      medium: :queue.len(medium),
      low: :queue.len(low),
      total: total_size,
      processed_count: processed_count,
      max_age: max_age,
      estimated_expired: expired_stats.estimated_expired,
      expired_ratio: expired_stats.expired_ratio
    }
  end

  @doc """
  获取消息的当前年龄（用于调试）
  """
  @spec message_age(t, aged_item) :: age
  def message_age(%__MODULE__{processed_count: count}, {_priority, _message, birth_count}) do
    count - birth_count + 1
  end

  @doc """
  清理所有过期消息（遍历所有优先级队列）
  返回 {expired_count, new_aggregate}

  注意：这个函数会完整遍历所有优先级队列，清理所有过期消息
  """
  @spec cleanup_expired(t) :: {non_neg_integer(), t}
  def cleanup_expired(aggregate) do
    # 分别清理每个优先级队列
    {expired_high, new_high} = cleanup_queue(aggregate.high, aggregate)
    {expired_medium, new_medium} = cleanup_queue(aggregate.medium, aggregate)
    {expired_low, new_low} = cleanup_queue(aggregate.low, aggregate)

    total_expired = expired_high + expired_medium + expired_low

    new_total_size = aggregate.total_size - total_expired

    new_aggregate = %{aggregate |
      high: new_high,
      medium: new_medium,
      low: new_low,
      total_size: new_total_size,
      # 如果清理后队列为空，重置 processed_count
      processed_count: if(new_total_size == 0, do: 0, else: aggregate.processed_count)
    }

    {total_expired, new_aggregate}
  end

  # 清理单个队列中的过期消息
  defp cleanup_queue(queue, %__MODULE__{processed_count: processed_count, max_age: max_age}) do
    cleanup_queue_recursive(queue, :queue.new(), processed_count, max_age, 0)
  end

  defp cleanup_queue_recursive(queue, new_queue, processed_count, max_age, expired_count) do
    case :queue.out(queue) do
      {{:value, {_priority, _message, birth_count} = item}, rest} ->
        age = processed_count - birth_count + 1

        if age > max_age do
          # 过期，跳过
          cleanup_queue_recursive(rest, new_queue, processed_count, max_age, expired_count + 1)
        else
          # 有效，保留
          cleanup_queue_recursive(rest, :queue.in(item, new_queue), processed_count, max_age, expired_count)
        end

      {:empty, _} ->
        {expired_count, new_queue}
    end
  end

  @doc """
  检查是否需要清理，并在必要时自动清理

  触发条件（满足任一即可）：

  1. 快速判断（基于队列大小和处理数）：
     - total_size > max_age * 3 并且
     - processed_count > max_age * 2
     说明：队列堆积超过3倍窗口，且已经处理了足够多消息，很可能有过期

  2. 精确判断（基于采样）：
     - total_size > 100 并且
     - 预估过期消息比例 > 50%
     说明：通过采样确认确实有大量过期消息

  返回 {cleaned, expired_count, new_aggregate}
  - cleaned: 是否执行了清理
  - expired_count: 清理的消息数
  """
  @spec maybe_cleanup(t) :: {boolean, non_neg_integer(), t}
  def maybe_cleanup(%__MODULE__{
    total_size: total_size,
    max_age: max_age,
    processed_count: processed_count
  } = aggregate) do

    # 策略1：基于队列大小的快速判断（更保守的触发条件）
    quick_check =
      total_size > max_age * 3 and
      processed_count > max_age * 2

    # 策略2：基于采样的精确判断
    should_cleanup = if quick_check do
      true
    else
      # 只有当快速检查不通过时，才进行采样（节省性能）
      stats = estimate_expired(aggregate)
      stats.total > 100 and stats.expired_ratio > 0.5
    end

    if should_cleanup do
      {expired_count, new_aggregate} = cleanup_expired(aggregate)
      {true, expired_count, new_aggregate}
    else
      {false, 0, aggregate}
    end
  end

  @doc """
  估算过期消息数量（通过采样）
  返回 %{estimated_expired: n, total: n, expired_ratio: 0.0-1.0}
  """
  @spec estimate_expired(t) :: %{
    estimated_expired: non_neg_integer(),
    total: non_neg_integer(),
    expired_ratio: float()
  }
  def estimate_expired(%__MODULE__{
    high: high,
    medium: medium,
    low: low,
    total_size: total_size,
    processed_count: processed_count,
    max_age: max_age
  }) do
    # 采样：检查每个队列的前10条消息
    sample_size = 10

    high_expired = count_expired_in_sample(high, processed_count, max_age, sample_size)
    medium_expired = count_expired_in_sample(medium, processed_count, max_age, sample_size)
    low_expired = count_expired_in_sample(low, processed_count, max_age, sample_size)

    high_total = min(:queue.len(high), sample_size)
    medium_total = min(:queue.len(medium), sample_size)
    low_total = min(:queue.len(low), sample_size)

    sampled_total = high_total + medium_total + low_total
    sampled_expired = high_expired + medium_expired + low_expired

    expired_ratio = if sampled_total > 0 do
      sampled_expired / sampled_total
    else
      0.0
    end

    estimated_expired = round(total_size * expired_ratio)

    %{
      estimated_expired: estimated_expired,
      total: total_size,
      expired_ratio: expired_ratio
    }
  end

  defp count_expired_in_sample(queue, processed_count, max_age, sample_size) do
    queue
    |> :queue.to_list()
    |> Enum.take(sample_size)
    |> Enum.count(fn {_priority, _message, birth_count} ->
      age = processed_count - birth_count + 1
      age > max_age
    end)
  end

  @doc """
  删除最老的消息（低 → 中 → 高）
  """
  @spec remove_oldest(t) :: t
  def remove_oldest(%__MODULE__{low: low, medium: medium, high: high, total_size: total_size} = aggregate) do
    cond do
      :queue.len(low) > 0 ->
        {{:value, _}, new_low} = :queue.out(low)
        %{aggregate | low: new_low, total_size: total_size - 1}

      :queue.len(medium) > 0 ->
        {{:value, _}, new_medium} = :queue.out(medium)
        %{aggregate | medium: new_medium, total_size: total_size - 1}

      :queue.len(high) > 0 ->
        {{:value, _}, new_high} = :queue.out(high)
        %{aggregate | high: new_high, total_size: total_size - 1}

      true ->
        aggregate
    end
  end

  defimpl Workex.Aggregate do
    defdelegate add(aggregate, message), to: Workex.AgedPriorityAggregate
    defdelegate size(aggregate), to: Workex.AgedPriorityAggregate
    defdelegate remove_oldest(aggregate), to: Workex.AgedPriorityAggregate

    def value(aggregate) do
      # 批量取出所有有效消息
      {messages, _expired_count, new_aggregate} = Workex.AgedPriorityAggregate.pop_batch(
        aggregate,
        Workex.AgedPriorityAggregate.size(aggregate)
      )

      {messages, %Workex.AgedPriorityAggregate{
        max_age: new_aggregate.max_age,
        processed_count: new_aggregate.processed_count
      }}
    end
  end
end
