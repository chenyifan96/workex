defmodule Workex do
  @moduledoc """
    A behaviour which separates message receiving and aggregating from message processing.

    Example:

      defmodule Consumer do
        use Workex

        # Interface functions are invoked inside client processes

        def start_link do
          Workex.start_link(__MODULE__, nil)
        end

        def push(pid, item) do
          Workex.push(pid, item)
        end


        # Callback functions run in the worker process

        def init(_), do: {:ok, nil}

        def handle(data, state) do
          Processor.long_op(data)
          {:ok, state}
        end
      end

  The `callback` module must export following functions:

  - init/1 - receives `arg` and should return `{:ok, initial_state}` or `{:stop, reason}`.
  - handle/2 - receives aggregated messages and the state, and should return `{:ok, new_state}`
    or `{:stop, reason}`.
  - handle_message/2 - optional message handler

  The `Workex` starts two processes. The one returned by `Workex.start_link/4` is the "facade"
  process which can be used as the target for messages. This is also the process which aggregates
  messages.

  Callback functions will run in the worker process, which is started by the "main" process.
  Thus, consuming is done concurrently to message aggregation.

  Both processes are linked, and the main process traps exits. Termination of the worker process
  will cause the main process to terminate with the same exit reason.
  """

  defstruct [
    :worker_pid,
    :aggregate,
    :worker_available,
    :max_size,
    :replace_oldest,
    pending_responses: MapSet.new,
    processing_responses: MapSet.new
  ]

  use GenServer

  @typep worker_state :: any
  @type workex_options ::
    [{:aggregate, Workex.Aggregate.t} |
    {:max_size, pos_integer} |
    {:replace_oldest, boolean}]
  @typep handle_response ::
  {:ok, worker_state} |
  {:ok, worker_state, pos_integer | :hibernate} |
  {:stop, reason :: any} |
  {:stop, reason :: any, worker_state}

  @callback init(any) :: {:ok, worker_state} | {:ok, worker_state, pos_integer | :hibernate} | {:stop, reason :: any}
  @callback handle(Workex.Aggregate.value, worker_state) :: handle_response
  @callback handle_message(any, worker_state) :: handle_response

  defmacro __using__(_) do
    quote do
      @behaviour unquote(__MODULE__)

      def handle_message(_, state), do: {:ok, state}
      defoverridable handle_message: 2
    end
  end

  alias Workex.Aggregate

  @doc """
  Starts aggregator and worker processes.

  See `start_link/3` for detailed description.
  """
  @spec start(module, any, workex_options | GenServer.options) :: GenServer.on_start
  def start(callback, arg, opts_or_gen_opts \\ []) do
    {opts, gen_server_opts} = extract_opts(opts_or_gen_opts)
    GenServer.start(__MODULE__, {callback, arg, opts}, gen_server_opts)
  end

  @doc """
  Starts aggregator and worker processes.

  Possible options are:

  - `aggregate` - Aggregation instance. Defaults to `%Workex.Queue{}`. Must implement `Workex.Aggregated`.
  - `max_size` - Maximum number of messages in the buffer after which new messages are discarded.
  - `replace_oldest` - Alters behavior of `max_size`. When the buffer is full, new message replaces the
    oldest one.

  Additional GenServer options (like `name:`) can be passed as the third argument.
  """
  @spec start_link(module, any, workex_options | GenServer.options) :: GenServer.on_start
  def start_link(callback, arg, opts_or_gen_opts \\ []) do
    {opts, gen_server_opts} = extract_opts(opts_or_gen_opts)
    GenServer.start_link(__MODULE__, {callback, arg, opts}, gen_server_opts)
  end

  # 从选项中分离 workex_options 和 gen_server_opts
  defp extract_opts(opts) when is_list(opts) do
    # GenServer 选项（如 name:, hibernate_after: 等）
    gen_server_keys = [:name, :hibernate_after, :spawn_opt, :debug, :timeout]

    {gen_server_opts, workex_opts} = Enum.split_with(opts, fn
      {key, _} -> key in gen_server_keys
      _ -> false
    end)

    {workex_opts, gen_server_opts}
  end

  defp extract_opts(opts), do: {opts, []}

  @impl true
  def init({callback, arg, opts}) do
    Process.flag(:trap_exit, true)
    case Workex.Worker.start_link(self(), callback, arg) do
      {:ok, worker_pid} ->
        {:ok, %__MODULE__{
          aggregate: opts[:aggregate] || %Workex.Queue{},
          worker_pid: worker_pid,
          max_size: opts[:max_size] || :unbound,
          replace_oldest: opts[:replace_oldest] || false,
          worker_available: true
        }}
      {:error, reason} ->
        {:stop, reason}
    end
  end

  @doc """
  Pushes a new message, returns immediately.
  """
  @spec push(GenServer.server, any) :: :ok
  def push(server, message) do
    GenServer.cast(server, {:push, message})
  end

  @doc """
  Pushes a new message and returns as soon as the message is queued (or rejected).
  """
  @spec push_ack(GenServer.server, any, non_neg_integer | :infinity) :: :ok | {:error, reason :: any}
  def push_ack(server, message, timeout \\ 5000) do
    GenServer.call(server, {:push_ack, message}, timeout)
  end

  @doc """
  Pushes a new message and returns after the message is processed (or rejected).
  """
  @spec push_block(GenServer.server, any, non_neg_integer | :infinity) :: :ok | {:error, reason :: any}
  def push_block(server, message, timeout \\ 5000) do
    GenServer.call(server, {:push_block, message}, timeout)
  end

  @impl true
  def handle_cast({:push, message}, state) do
    {_, new_state} = add_and_notify(state, message)
    {:noreply, new_state}
  end

  @impl true
  def handle_call({:push_ack, message}, from, state) do
    {response, new_state} = add_and_notify(state, message, from)
    {:reply, response, new_state}
  end

  @impl true
  def handle_call({:push_block, message}, from, state) do
    {response, new_state} = add_and_notify(state, message, from)
    case response do
      :ok -> {:noreply, new_state}
      _ -> {:reply, response, new_state}
    end
  end

  defp add_and_notify(
    %__MODULE__{
      aggregate: aggregate,
      max_size: max_size,
      worker_available: worker_available,
      replace_oldest: replace_oldest
    } = state,
    message,
    from \\ nil
  ) do
    current_size = Aggregate.size(aggregate)

    # 检查容量限制：如果设置了 max_size 且队列已满，且 worker 不可用
    if max_size != :unbound and current_size >= max_size and not worker_available do
      if replace_oldest do
        aggregate
        |> Aggregate.remove_oldest
        |> Aggregate.add(message)
        |> handle_add(state, from)
      else
        {{:error, :max_capacity}, state}
      end
    else
      aggregate
      |> Aggregate.add(message)
      |> handle_add(state, from)
    end
  end

  defp handle_add(add_result, state, from) do
    case add_result do
      {:ok, new_aggregate} ->
        {:ok,
          %__MODULE__{state | aggregate: new_aggregate}
          |> add_pending_response(from)
          |> maybe_notify_worker
        }
      error ->
        {error, state}
    end
  end

  defp add_pending_response(state, nil), do: state
  defp add_pending_response(%__MODULE__{pending_responses: pending_responses} = state, from) do
    %__MODULE__{state | pending_responses: MapSet.put(pending_responses, from)}
  end


  defp maybe_notify_worker(
    %__MODULE__{
      worker_available: true,
      worker_pid: worker_pid,
      aggregate: aggregate,
      pending_responses: pending_responses
    } = state
  ) do
    unless Aggregate.size(aggregate) == 0 do
      {payload, aggregate} = Aggregate.value(aggregate)
      GenServer.cast(worker_pid, {:process, payload})
      %__MODULE__{state |
        worker_available: false,
        aggregate: aggregate,
        processing_responses: pending_responses,
        pending_responses: MapSet.new
      }
    else
      state
    end
  end

  defp maybe_notify_worker(state), do: state


  @impl true
  def handle_info({:workex, :worker_available}, state) do
    new_state = %__MODULE__{state | worker_available: true}
      |> notify_pending
      |> maybe_notify_worker
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:EXIT, worker_pid, reason}, %__MODULE__{worker_pid: worker_pid} = state) do
    {:stop, reason, state}
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, %__MODULE__{
    aggregate: _aggregate,
    worker_pid: _worker_pid,
    processing_responses: processing_responses,
    pending_responses: pending_responses
  }) do
    # 清理：通知所有待处理的响应
    # 由于进程被终止，消息可能没有被处理，应该回复错误

    # 1. 正在处理的响应（processing_responses）
    # 这些是 push_block 的响应，消息正在处理但进程被终止
    # 注意：如果 worker 在终止前已经完成处理并调用了 notify_pending，
    # 这些响应可能已经被回复，但为了安全起见，我们仍然尝试回复
    # GenServer.reply/2 对于已经回复的响应会抛出错误，需要捕获
    Enum.each(processing_responses, fn from ->
      try do
        GenServer.reply(from, {:error, :shutdown})
      rescue
        ArgumentError -> :ok  # 响应已经被回复，忽略错误
      end
    end)

    # 2. 等待入队的响应（pending_responses）
    # 这些是 push_block 的响应，消息已入队但进程被终止，消息会被丢弃
    Enum.each(pending_responses, fn from ->
      try do
        GenServer.reply(from, {:error, :shutdown})
      rescue
        ArgumentError -> :ok  # 响应已经被回复，忽略错误
      end
    end)

    # 注意：队列中的消息会被丢弃
    # 如果需要处理剩余消息，可以在这里添加逻辑
    # 例如：等待 worker 处理完当前消息，或者将剩余消息保存到其他地方

    # Worker 进程会因为链接关系自动退出
    :ok
  end

  defp notify_pending(%__MODULE__{processing_responses: processing_responses} = state) do
    Enum.each(processing_responses, &GenServer.reply(&1, :ok))
    %__MODULE__{state | processing_responses: MapSet.new}
  end
end
