defmodule ConsumerTest.Producer do
  use GenStage

  def start_link(_) do
    GenStage.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def init(_) do
    {:producer, %{items: [], pending_demand: 0, db_queue: Enum.to_list(1..17)}}
  end

  def handle_demand(new_demand, %{items: items, pending_demand: pending_demand} = state)
      when new_demand > 0 do
    demand = new_demand + pending_demand
    {to_send, rest} = Enum.split(items, demand)

    unfulfilled_demand = demand - length(to_send)
    # If there's less items to send, we will dispatch after polling the DB
    # We also make sure to increase the pending_demand by the unfulfilled demand
    if unfulfilled_demand > 0 do
      Process.send_after(self(), :poll, 2000)
    end

    {:noreply, to_send, Map.merge(state, %{items: rest, pending_demand: unfulfilled_demand})}
  end

  def handle_info(:poll, state) do
    case get_report_items(state) do
      {[], state} ->
        # No new items to send, we will poll again
        # In prod, we can wait for atleast 5 minutes before polling again
        Process.send_after(self(), :poll, 2000)
        {:noreply, [], state}

      {items, state} ->
        # We have new items to send.
        # We send the items and subtract from pending_demand.
        # The rest of the items are added to the state, so when the next handle_demand is called
        # and we have sufficient items to meet the demand, we dont have to poll.
        {to_send, rest} = Enum.split(items, state.pending_demand)
        unfulfilled_demand = state.pending_demand - length(to_send)

        if unfulfilled_demand > 0 do
          Process.send_after(self(), :poll, 2000)
        end

        {:noreply, to_send, Map.merge(state, %{items: rest, pending_demand: unfulfilled_demand})}
    end
  end

  defp get_report_items(%{db_queue: db_queue} = state) do
    items =
      if Enum.empty?(db_queue) do
        []
      else
        # in produze repo also we take a max of 10 items per db call
        # it can be any number, but large enough to not make too many db calls
        # and small enough to not block other nodes from getting data
        # so as to maximize the parallelism of the system

        Enum.take(db_queue, 10)
      end

    seconds = :second |> DateTime.utc_now() |> DateTime.to_unix() |> rem(100)
    IO.inspect("Retrieved #{length(items)} items at #{inspect(seconds)}")

    {items, %{state | db_queue: db_queue -- items}}
  end
end
