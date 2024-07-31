defmodule ConsumerTest.Producer do
  use GenStage

  @buffer_size 10

  def start_link(_) do
    GenStage.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def init(_) do
    Process.send_after(self(), :poll, 2000)
    {:producer, %{items: [], pending_demand: 0, db_queue: Enum.to_list(1..1000)}}
  end

  def handle_demand(new_demand, %{items: items, pending_demand: pending_demand} = state)
      when new_demand > 0 do
    demand = new_demand + pending_demand
    {to_send, rest} = Enum.split(items, demand)

    unfulfilled_demand = demand - length(to_send)

    IO.inspect("Handling new demand: #{new_demand}, unfulfilled_demand: #{unfulfilled_demand}")

    {:noreply, to_send, Map.merge(state, %{items: rest, pending_demand: unfulfilled_demand})}
  end

  def handle_info(:poll, state) do
    {to_send, state} =
      case get_report_items(state) do
        {[], state} ->
          # Nothing to send in this poll
          {[], state}

        {items, state} ->
          {to_send, rest} = Enum.split(items, state.pending_demand)
          unfulfilled_demand = state.pending_demand - length(to_send)
          state = Map.merge(state, %{items: rest, pending_demand: unfulfilled_demand})

          # We have items to send. Send upto pending_demand items
          {to_send, state}
      end

    # Query the DB after an interval
    Process.send_after(self(), :poll, 2000)
    {:noreply, to_send, state}
  end

  defp get_report_items(%{db_queue: db_queue} = state) do
    # max we can fetch from the DB is the buffer size - the current items in the buffer
    n = @buffer_size - length(state.items)
    {items, rest} = Enum.split(db_queue, n)
    seconds = DateTime.utc_now().second

    IO.inspect(
      "Retrieved #{length(items)} items at #{inspect(seconds)}. Remaining items: #{length(rest)}. Pending demand: #{state.pending_demand}"
    )

    {items, %{state | db_queue: rest}}
  end
end
