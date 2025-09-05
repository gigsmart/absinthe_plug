defmodule Absinthe.Plug.Incremental.SSE.ConnectionManager do
  @moduledoc """
  Manages SSE connection lifecycle and headers.
  
  This module handles connection setup, keep-alive functionality,
  and proper SSE header configuration.
  """

  import Plug.Conn

  @content_type "text/event-stream"
  @keep_alive_interval 30_000  # 30 seconds

  @doc """
  Check if the client accepts SSE responses.
  """
  @spec accepts_sse?(Plug.Conn.t()) :: boolean()
  def accepts_sse?(conn) do
    case get_req_header(conn, "accept") do
      [] -> false
      headers ->
        Enum.any?(headers, fn header ->
          String.contains?(header, "text/event-stream") or
          String.contains?(header, "*/*")
        end)
    end
  end

  @doc """
  Setup proper SSE headers on the connection.
  """
  @spec setup_sse_headers(Plug.Conn.t()) :: Plug.Conn.t()
  def setup_sse_headers(conn) do
    conn
    |> put_resp_header("content-type", @content_type)
    |> put_resp_header("cache-control", "no-cache")
    |> put_resp_header("connection", "keep-alive")
    |> put_resp_header("x-accel-buffering", "no")  # Disable Nginx buffering
    |> put_resp_header("access-control-allow-origin", "*")  # CORS support
    |> put_resp_header("access-control-allow-headers", "cache-control")
    |> send_chunked(200)
  end

  @doc """
  Schedule a keep-alive message to prevent connection timeout.
  """
  @spec schedule_keep_alive() :: reference()
  def schedule_keep_alive do
    Process.send_after(self(), :keep_alive, @keep_alive_interval)
  end
end