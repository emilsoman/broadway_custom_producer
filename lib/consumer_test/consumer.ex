defmodule ConsumerTest.Consumer do
  alias Broadway.Message
  use Broadway

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {ConsumerTest.Producer, []},
        concurrency: 1,
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [concurrency: 1]
      ],
      batchers: [
        default: [concurrency: 2, batch_size: 5, batch_timeout: 5000]
      ],
      partition_by: &partition/1
    )
  end

  @impl true
  def handle_message(_, %Message{} = message, _) do
    message
  end

  def partition(%Message{} = message) do
    message.data
  end

  @impl true
  def handle_batch(:default, messages, _batch_info, _context) do
    IO.inspect("Processing batch of #{Enum.count(messages)} items")

    Enum.map(messages, fn %Message{data: _item} = message ->
      # Processing each message can take a random amount of time
      random_number = Enum.random(2..20) * 1000
      Process.sleep(random_number)

      message
    end)
  end

  def transform(item, _opts) do
    %Message{
      data: item,
      acknowledger: {__MODULE__, :item, []}
    }
  end

  def ack(:item, _successful, _failed) do
    :ok
  end
end
