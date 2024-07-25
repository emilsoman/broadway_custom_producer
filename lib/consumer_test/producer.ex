defmodule ConsumerTest.Producer do
  use GenStage

  def start_link(_) do
    GenStage.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def init(_) do
    # state = get_report_items()
    {:producer, %{items: [], pending_demand: 0}}
  end

  def handle_demand(new_demand, %{items: items, pending_demand: pending_demand})
      when new_demand > 0 do
    demand = new_demand + pending_demand
    {to_send, rest} = Enum.split(items, demand)

    unfulfilled_demand = demand - length(to_send)
    # If there's less items to send, we will dispatch after polling the DB
    # We also make sure to increase the pending_demand by the unfulfilled demand
    if unfulfilled_demand > 0 do
      Process.send_after(self(), :poll, 2000)
    end

    {:noreply, to_send, %{items: rest, pending_demand: unfulfilled_demand}}
  end

  def handle_info(:poll, state) do
    case get_report_items() do
      [] ->
        # No new items to send, we will poll again
        # In prod, we can wait for atleast 5 minutes before polling again
        Process.send_after(self(), :poll, 2000)
        {:noreply, [], state}

      items ->
        # We have new items to send.
        # We send the items and subtract from pending_demand.
        # The rest of the items are added to the state, so when the next handle_demand is called
        # and we have sufficient items to meet the demand, we dont have to poll.
        {to_send, rest} = Enum.split(items, state.pending_demand)
        unfulfilled_demand = state.pending_demand - length(to_send)

        if unfulfilled_demand > 0 do
          Process.send_after(self(), :poll, 2000)
        end

        {:noreply, to_send, %{items: rest, pending_demand: unfulfilled_demand}}
    end
  end

  defp get_report_items do
    IO.inspect("Calling get_report_items #{inspect(DateTime.utc_now(:second))}")
    # Most of the time there's nothing new to return,
    # but sometimes we return a list of items
    if Enum.random(1..10) > 8 do
      Enum.to_list(1..30)
    else
      []
    end
  end
end
