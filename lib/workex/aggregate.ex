defprotocol Workex.Aggregate do
  @moduledoc """
  Specifies the protocol used by `Workex` behaviour to aggregate incoming messages.
  """

  @type value :: any

  @doc "Adds the new item to the aggregate."
  @spec add(t, any) :: t
  def add(aggregate, message)

  @doc """
    Produces an aggregated value from all collected items.

    The returned tuple contains aggregated items, and the new instance that doesn't
    contain those items.
  """
  @spec value(t) :: {value, t}
  def value(aggregate)

  @doc """
  Returns the number of aggregated items.

  This function is invoked frequently, so be sure to make the implementation fast.
  """
  @spec size(t) :: non_neg_integer
  def size(aggregate)

  @doc """
  Removes the oldest item from the collection.

  Sometimes it doesn't make sense to implement this function, for example when the
  aggregation doesn't guarantee or preserve ordering. In such cases, just raise from
  the implementation, and document that the implementation can't be used with the
  `replace_oldest` option.
  """
  @spec remove_oldest(t) :: t
  def remove_oldest(aggregate)
end


defmodule Workex.Stack do
  @moduledoc """
  Aggregates messages in the stack like fashion. The aggregated value will contain
  newer messages first.

  支持一条一条读取消息（pop 方法），适合限流场景。
  """
  defstruct items: :queue.new, size: 0

  @type t :: %__MODULE__{
    items: :queue.queue(),
    size: non_neg_integer()
  }

  @doc false
  def add(%__MODULE__{items: items, size: size} = stack, message) do
    {:ok, %__MODULE__{stack | items: :queue.in_r(message, items), size: size + 1}}
  end

  @doc false
  def value(%__MODULE__{items: items}) do
    {:queue.to_list(items), %__MODULE__{}}
  end

  @doc false
  def size(%__MODULE__{size: size}), do: size

  @doc false
  def remove_oldest(%__MODULE__{items: items, size: size} = stack) do
    {_, items} = :queue.out_r(items)
    %__MODULE__{stack | items: items, size: size - 1}
  end

  @doc """
  按 LIFO 顺序（后进先出）取出一条消息。
  返回 {{:value, message}, new_stack} 或 {:empty, stack}
  """
  @spec pop(t) :: {{:value, any}, t} | {:empty, t}
  def pop(%__MODULE__{items: items, size: size} = stack) do
    case :queue.out_r(items) do
      {{:value, message}, new_items} ->
        new_stack = %__MODULE__{stack | items: new_items, size: size - 1}
        {{:value, message}, new_stack}

      {:empty, _} ->
        {:empty, stack}
    end
  end

  @doc """
  批量取出消息（LIFO 顺序）。
  返回 {messages, new_stack}
  """
  @spec pop_batch(t, pos_integer()) :: {list(any), t}
  def pop_batch(stack, max_count \\ 20) do
    pop_batch_recursive(stack, max_count, [])
  end

  defp pop_batch_recursive(stack, 0, acc) do
    {Enum.reverse(acc), stack}
  end

  defp pop_batch_recursive(stack, remaining, acc) do
    case pop(stack) do
      {{:value, message}, new_stack} ->
        pop_batch_recursive(new_stack, remaining - 1, [message | acc])

      {:empty, stack} ->
        {Enum.reverse(acc), stack}
    end
  end

  @doc """
  获取统计信息
  """
  @spec stats(t) :: %{total: non_neg_integer()}
  def stats(%__MODULE__{size: size}) do
    %{total: size}
  end

  defimpl Workex.Aggregate do
    defdelegate add(aggregate, message), to: Workex.Stack
    defdelegate value(aggregate), to: Workex.Stack
    defdelegate size(aggregate), to: Workex.Stack
    defdelegate remove_oldest(aggregate), to: Workex.Stack
  end
end


defmodule Workex.Queue do
  @moduledoc """
  Aggregates messages in the queue like fashion. The aggregated value will be a list
  that preserves the order of messages.

  支持一条一条读取消息（pop 方法），适合限流场景。
  """
  defstruct items: :queue.new, size: 0

  @type t :: %__MODULE__{
    items: :queue.queue(),
    size: non_neg_integer()
  }

  @doc false
  def add(%__MODULE__{items: items, size: size} = queue, message) do
    {:ok, %__MODULE__{queue | items: :queue.in(message, items), size: size + 1}}
  end

  @doc false
  def value(%__MODULE__{} = queue) do
    # 使用 pop 方法一条一条读取消息（与 AgedPriorityAggregate 保持一致）
    case pop(queue) do
      {{:value, message}, new_queue} ->
        {[message], new_queue}
      {:empty, queue} ->
        {[], queue}
    end
  end

  @doc false
  def size(%__MODULE__{size: size}), do: size

  @doc false
  def remove_oldest(%__MODULE__{items: items, size: size} = queue) do
    {_, items} = :queue.out(items)
    %__MODULE__{queue | items: items, size: size - 1}
  end

  @doc """
  按 FIFO 顺序取出一条消息。
  返回 {{:value, message}, new_queue} 或 {:empty, queue}
  """
  @spec pop(t) :: {{:value, any}, t} | {:empty, t}
  def pop(%__MODULE__{items: items, size: size} = queue) do
    case :queue.out(items) do
      {{:value, message}, new_items} ->
        new_queue = %__MODULE__{queue | items: new_items, size: size - 1}
        {{:value, message}, new_queue}

      {:empty, _} ->
        {:empty, queue}
    end
  end

  @doc """
  批量取出消息。
  返回 {messages, new_queue}
  """
  @spec pop_batch(t, pos_integer()) :: {list(any), t}
  def pop_batch(queue, max_count \\ 20) do
    pop_batch_recursive(queue, max_count, [])
  end

  defp pop_batch_recursive(queue, 0, acc) do
    {Enum.reverse(acc), queue}
  end

  defp pop_batch_recursive(queue, remaining, acc) do
    case pop(queue) do
      {{:value, message}, new_queue} ->
        pop_batch_recursive(new_queue, remaining - 1, [message | acc])

      {:empty, queue} ->
        {Enum.reverse(acc), queue}
    end
  end

  @doc """
  获取统计信息
  """
  @spec stats(t) :: %{total: non_neg_integer()}
  def stats(%__MODULE__{size: size}) do
    %{total: size}
  end

  defimpl Workex.Aggregate do
    defdelegate add(aggregate, message), to: Workex.Queue
    defdelegate value(aggregate), to: Workex.Queue
    defdelegate size(aggregate), to: Workex.Queue
    defdelegate remove_oldest(aggregate), to: Workex.Queue
  end
end


defmodule Workex.Dict do
  @moduledoc """
  Assumes that messages are key-value pairs. The new message will overwrite the
  existing one of the same key. The aggregated value is a list of key-value tuples.
  Ordering is not preserved.
  """
  defstruct items: Map.new

  @doc false
  def add(%__MODULE__{items: items} = dict, {key, value}) do
    {:ok, %__MODULE__{dict | items: Map.put(items, key, value)}}
  end

  @doc false
  def value(%__MODULE__{items: items}) do
    {Map.to_list(items), %__MODULE__{}}
  end

  @doc false
  def size(%__MODULE__{items: items}), do: map_size(items)

  defimpl Workex.Aggregate do
    defdelegate add(aggregate, message), to: Workex.Dict
    defdelegate value(aggregate), to: Workex.Dict
    defdelegate size(aggregate), to: Workex.Dict
    def remove_oldest(_), do: raise("not implemented")
  end
end
