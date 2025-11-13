defmodule Workex.PriorityAggregate do
  @moduledoc """
  优先级聚合器：支持高、中、低三个优先级的消息队列。

  特性：
  - 消息格式：{:high | :medium | :low, message}
  - 总容量限制（通过 max_size 配置）
  - 队列满时的智能挤出策略：
    * 新的中优先级消息：挤出最老的低优先级
    * 新的高优先级消息：优先挤出最老的低优先级，没有低的再挤出最老的中优先级
  - 处理时按优先级排序：高 → 中 → 低

  使用示例：

      {:ok, pid} = Workex.start_link(
        MyWorker,
        nil,
        aggregate: %Workex.PriorityAggregate{},
        max_size: 20
      )

      Workex.push(pid, {:high, "重要消息"})
      Workex.push(pid, {:medium, "普通消息"})
      Workex.push(pid, {:low, "低优先级消息"})
  """

  defstruct high: :queue.new(),
            medium: :queue.new(),
            low: :queue.new(),
            total_size: 0

  @type priority :: :high | :medium | :low
  @type message :: any
  @type t :: %__MODULE__{
    high: :queue.queue(),
    medium: :queue.queue(),
    low: :queue.queue(),
    total_size: non_neg_integer()
  }

  @doc """
  添加消息到队列。
  消息格式必须是 {priority, message}，其中 priority 为 :high | :medium | :low
  """
  @spec add(t, {priority, message}) :: {:ok, t} | {:error, term}
  def add(%__MODULE__{} = aggregate, {priority, _message} = item)
      when priority in [:high, :medium, :low] do
    new_aggregate = case priority do
      :high ->
        %{aggregate |
          high: :queue.in(item, aggregate.high),
          total_size: aggregate.total_size + 1
        }
      :medium ->
        %{aggregate |
          medium: :queue.in(item, aggregate.medium),
          total_size: aggregate.total_size + 1
        }
      :low ->
        %{aggregate |
          low: :queue.in(item, aggregate.low),
          total_size: aggregate.total_size + 1
        }
    end

    {:ok, new_aggregate}
  end

  def add(_aggregate, _item) do
    {:error, :invalid_message_format}
  end

  @doc """
  提取所有消息，按优先级顺序返回：高 → 中 → 低
  返回 {messages, empty_aggregate}
  """
  @spec value(t) :: {list({priority, message}), t}
  def value(%__MODULE__{high: high, medium: medium, low: low}) do
    messages =
      :queue.to_list(high) ++
      :queue.to_list(medium) ++
      :queue.to_list(low)

    {messages, %__MODULE__{}}
  end

  @doc """
  返回当前队列中的总消息数
  """
  @spec size(t) :: non_neg_integer()
  def size(%__MODULE__{total_size: total_size}) do
    total_size
  end

  @doc """
  删除最老的消息，按优先级从低到高删除：低 → 中 → 高
  """
  @spec remove_oldest(t) :: t
  def remove_oldest(%__MODULE__{low: low, medium: medium, high: high, total_size: total_size} = aggregate) do
    cond do
      # 优先删除低优先级的最老消息
      :queue.len(low) > 0 ->
        {{:value, _}, new_low} = :queue.out(low)
        %{aggregate | low: new_low, total_size: total_size - 1}

      # 其次删除中优先级的最老消息
      :queue.len(medium) > 0 ->
        {{:value, _}, new_medium} = :queue.out(medium)
        %{aggregate | medium: new_medium, total_size: total_size - 1}

      # 最后删除高优先级的最老消息
      :queue.len(high) > 0 ->
        {{:value, _}, new_high} = :queue.out(high)
        %{aggregate | high: new_high, total_size: total_size - 1}

      # 队列为空
      true ->
        aggregate
    end
  end

  @doc """
  按优先级顺序（高 → 中 → 低）取出一条消息
  返回 {{:value, item}, new_aggregate} 或 {:empty, aggregate}
  """
  @spec pop(t) :: {{:value, {priority, message}}, t} | {:empty, t}
  def pop(%__MODULE__{high: high, medium: medium, low: low, total_size: total_size} = aggregate) do
    cond do
      # 优先从高优先级队列取出
      :queue.len(high) > 0 ->
        {{:value, item}, new_high} = :queue.out(high)
        new_aggregate = %{aggregate | high: new_high, total_size: total_size - 1}
        {{:value, item}, new_aggregate}

      # 其次从中优先级队列取出
      :queue.len(medium) > 0 ->
        {{:value, item}, new_medium} = :queue.out(medium)
        new_aggregate = %{aggregate | medium: new_medium, total_size: total_size - 1}
        {{:value, item}, new_aggregate}

      # 最后从低优先级队列取出
      :queue.len(low) > 0 ->
        {{:value, item}, new_low} = :queue.out(low)
        new_aggregate = %{aggregate | low: new_low, total_size: total_size - 1}
        {{:value, item}, new_aggregate}

      # 所有队列都为空
      true ->
        {:empty, aggregate}
    end
  end

  @doc """
  获取各优先级队列的统计信息
  """
  @spec stats(t) :: %{high: non_neg_integer(), medium: non_neg_integer(), low: non_neg_integer(), total: non_neg_integer()}
  def stats(%__MODULE__{high: high, medium: medium, low: low, total_size: total_size}) do
    %{
      high: :queue.len(high),
      medium: :queue.len(medium),
      low: :queue.len(low),
      total: total_size
    }
  end

  defimpl Workex.Aggregate do
    defdelegate add(aggregate, message), to: Workex.PriorityAggregate
    defdelegate value(aggregate), to: Workex.PriorityAggregate
    defdelegate size(aggregate), to: Workex.PriorityAggregate
    defdelegate remove_oldest(aggregate), to: Workex.PriorityAggregate
  end
end
