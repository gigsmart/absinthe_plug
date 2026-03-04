defmodule Absinthe.Plug.Incremental.SSE.ConnectionManagerTest do
  use ExUnit.Case, async: true
  import Plug.Test
  import Plug.Conn

  alias Absinthe.Plug.Incremental.SSE.ConnectionManager

  describe "accepts_sse?/1" do
    test "returns true for text/event-stream accept header" do
      conn = conn(:get, "/") |> put_req_header("accept", "text/event-stream")
      assert ConnectionManager.accepts_sse?(conn)
    end

    test "returns true for wildcard accept header" do
      conn = conn(:get, "/") |> put_req_header("accept", "*/*")
      assert ConnectionManager.accepts_sse?(conn)
    end

    test "returns true for mixed accept header containing text/event-stream" do
      conn = conn(:get, "/") |> put_req_header("accept", "application/json, text/event-stream")
      assert ConnectionManager.accepts_sse?(conn)
    end

    test "returns false for no accept header" do
      conn = conn(:get, "/")
      refute ConnectionManager.accepts_sse?(conn)
    end

    test "returns false for non-SSE accept header" do
      conn = conn(:get, "/") |> put_req_header("accept", "application/json")
      refute ConnectionManager.accepts_sse?(conn)
    end
  end

  describe "setup_sse_headers/1" do
    test "sets content-type to text/event-stream" do
      conn = conn(:get, "/") |> ConnectionManager.setup_sse_headers()
      assert get_resp_header(conn, "content-type") == ["text/event-stream"]
    end

    test "sets cache-control to no-cache" do
      conn = conn(:get, "/") |> ConnectionManager.setup_sse_headers()
      assert get_resp_header(conn, "cache-control") == ["no-cache"]
    end

    test "sets connection to keep-alive" do
      conn = conn(:get, "/") |> ConnectionManager.setup_sse_headers()
      assert get_resp_header(conn, "connection") == ["keep-alive"]
    end

    test "disables nginx buffering" do
      conn = conn(:get, "/") |> ConnectionManager.setup_sse_headers()
      assert get_resp_header(conn, "x-accel-buffering") == ["no"]
    end

    test "sends chunked response with 200 status" do
      conn = conn(:get, "/") |> ConnectionManager.setup_sse_headers()
      assert conn.status == 200
      assert conn.state == :chunked
    end
  end

  describe "schedule_keep_alive/0" do
    test "sends :keep_alive message after interval" do
      ref = ConnectionManager.schedule_keep_alive()
      assert is_reference(ref)
      # Cancel the timer to avoid message leak in tests
      Process.cancel_timer(ref)
    end
  end
end
