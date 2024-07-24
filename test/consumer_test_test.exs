defmodule ConsumerTestTest do
  use ExUnit.Case
  doctest ConsumerTest

  test "greets the world" do
    assert ConsumerTest.hello() == :world
  end
end
