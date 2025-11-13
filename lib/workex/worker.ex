defmodule Workex.Worker do
  @moduledoc false
  use GenServer

  defstruct [:queue_pid, :callback, :state]

  def start(queue_pid, callback, arg) do
    GenServer.start(__MODULE__, {queue_pid, callback, arg})
  end

  def start_link(queue_pid, callback, arg) do
    GenServer.start_link(__MODULE__, {queue_pid, callback, arg})
  end

  @impl true
  def init({queue_pid, callback, arg}) do
    case callback.init(arg) do
      {:ok, state} ->
        {:ok, %__MODULE__{
          queue_pid: queue_pid,
          callback: callback,
          state: state
        }}

      {:ok, state, timeout} ->
        {:ok, %__MODULE__{
          queue_pid: queue_pid,
          callback: callback,
          state: state
        }, timeout}

      other -> other
    end
  end

  @impl true
  def handle_cast({:process, messages}, %__MODULE__{callback: callback, state: state} = worker_state) do
    response = callback.handle(messages, state)
    handle_response(response, worker_state)
  end

  @impl true
  def handle_info(message, %__MODULE__{callback: callback, state: state} = worker_state) do
    response = callback.handle_message(message, state)
    handle_response(response, worker_state)
  end

  defp handle_response(
    response,
    %__MODULE__{queue_pid: queue_pid, state: state} = worker_state
  ) do
    case response do
      {:ok, new_state} ->
        send(queue_pid, {:workex, :worker_available})
        {:noreply, %__MODULE__{worker_state | state: new_state}}

      {:ok, new_state, timeout_or_hibernate} ->
        send(queue_pid, {:workex, :worker_available})
        {:noreply, %__MODULE__{worker_state | state: new_state}, timeout_or_hibernate}

      {:stop, reason, new_state} ->
        {:stop, reason, %__MODULE__{worker_state | state: new_state}}

      {:stop, reason} ->
        {:stop, reason, state}
    end
  end
end
