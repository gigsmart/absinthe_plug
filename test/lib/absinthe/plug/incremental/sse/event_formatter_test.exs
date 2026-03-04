defmodule Absinthe.Plug.Incremental.SSE.EventFormatterTest do
  use ExUnit.Case, async: true

  alias Absinthe.Plug.Incremental.SSE.EventFormatter

  describe "format_event/3" do
    test "formats initial event with correct SSE structure" do
      data = %{data: %{users: [%{name: "Alice"}]}}
      result = EventFormatter.format_event("initial", data, 0)

      assert result =~ "id: 0\n"
      assert result =~ "event: initial\n"
      assert result =~ "data: "
      assert result =~ "\n\n"

      # Verify the data line contains valid JSON
      [_, _, data_line, _] = String.split(result, "\n", parts: 4)
      json = String.replace_prefix(data_line, "data: ", "")
      assert {:ok, decoded} = Jason.decode(json)
      assert decoded["data"]["users"] == [%{"name" => "Alice"}]
    end

    test "formats incremental event" do
      data = %{incremental: [%{data: %{profile: %{bio: "Hello"}}, path: ["users", 0]}]}
      result = EventFormatter.format_event("incremental", data, 5)

      assert result =~ "id: 5\n"
      assert result =~ "event: incremental\n"
    end

    test "formats complete event" do
      result = EventFormatter.format_event("complete", %{}, 10)

      assert result =~ "id: 10\n"
      assert result =~ "event: complete\n"
      assert result =~ "data: {}\n"
    end

    test "increments event IDs correctly" do
      result0 = EventFormatter.format_event("initial", %{}, 0)
      result1 = EventFormatter.format_event("incremental", %{}, 1)

      assert result0 =~ "id: 0\n"
      assert result1 =~ "id: 1\n"
    end
  end

  describe "format_error_response/1" do
    test "wraps string error in errors list" do
      result = EventFormatter.format_error_response("something went wrong")
      assert result == %{errors: [%{message: "something went wrong"}]}
    end

    test "wraps map error in errors list" do
      error = %{message: "field not found", locations: [%{line: 1, column: 5}]}
      result = EventFormatter.format_error_response(error)
      assert result == %{errors: [error]}
    end

    test "passes through error list directly" do
      errors = [%{message: "error 1"}, %{message: "error 2"}]
      result = EventFormatter.format_error_response(errors)
      assert result == %{errors: errors}
    end

    test "inspects unknown error types" do
      result = EventFormatter.format_error_response({:badarg, "oops"})
      assert %{errors: [%{message: msg}]} = result
      assert msg =~ "badarg"
    end
  end
end
