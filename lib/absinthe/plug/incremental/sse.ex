defmodule Absinthe.Plug.Incremental.SSE do
  @moduledoc """
  Server-Sent Events (SSE) transport for incremental delivery.
  
  This module implements incremental delivery over HTTP using SSE,
  allowing @defer and @stream directives to work over standard HTTP connections.
  """
  
  use Absinthe.Incremental.Transport
  import Plug.Conn
  
  require Logger
  
  @content_type "text/event-stream"
  @keep_alive_interval 30_000  # 30 seconds
  
  @impl true
  def init(conn, options) do
    # Validate that the client accepts SSE
    if accepts_sse?(conn) do
      conn = 
        conn
        |> put_resp_header("content-type", @content_type)
        |> put_resp_header("cache-control", "no-cache")
        |> put_resp_header("connection", "keep-alive")
        |> put_resp_header("x-accel-buffering", "no")  # Disable Nginx buffering
        |> send_chunked(200)
      
      # Start keep-alive timer if configured
      if Keyword.get(options, :keep_alive, true) do
        schedule_keep_alive()
      end
      
      {:ok, %{
        conn: conn,
        operation_id: Keyword.get(options, :operation_id),
        event_id: 0,
        options: options
      }}
    else
      {:error, :sse_not_accepted}
    end
  end
  
  @impl true
  def send_initial(state, response) do
    event_data = format_event("initial", response, state.event_id)
    
    case chunk(state.conn, event_data) do
      {:ok, conn} ->
        {:ok, %{state | 
          conn: conn, 
          event_id: state.event_id + 1
        }}
        
      {:error, reason} ->
        Logger.error("Failed to send initial SSE response: #{inspect(reason)}")
        {:error, {:transport_error, reason}}
    end
  end
  
  @impl true
  def send_incremental(state, response) do
    event_data = format_event("incremental", response, state.event_id)
    
    case chunk(state.conn, event_data) do
      {:ok, conn} ->
        {:ok, %{state | 
          conn: conn, 
          event_id: state.event_id + 1
        }}
        
      {:error, reason} ->
        Logger.error("Failed to send incremental SSE response: #{inspect(reason)}")
        {:error, {:transport_error, reason}}
    end
  end
  
  @impl true
  def complete(state) do
    # Send completion event
    event_data = format_event("complete", %{}, state.event_id)
    
    case chunk(state.conn, event_data) do
      {:ok, conn} ->
        # Close the connection
        chunk(conn, "")
        :ok
        
      {:error, reason} ->
        Logger.error("Failed to send complete SSE event: #{inspect(reason)}")
        {:error, {:transport_error, reason}}
    end
  end
  
  @impl true
  def handle_error(state, error) do
    error_response = format_error_response(error)
    event_data = format_event("error", error_response, state.event_id)
    
    case chunk(state.conn, event_data) do
      {:ok, conn} ->
        {:ok, %{state | 
          conn: conn, 
          event_id: state.event_id + 1
        }}
        
      {:error, reason} ->
        Logger.error("Failed to send error SSE event: #{inspect(reason)}")
        {:error, {:transport_error, reason}}
    end
  end
  
  @doc """
  Handle keep-alive to prevent connection timeout.
  """
  def handle_keep_alive(state) do
    # Send a comment to keep the connection alive
    case chunk(state.conn, ": keep-alive\n\n") do
      {:ok, conn} ->
        # Schedule next keep-alive
        schedule_keep_alive()
        {:ok, %{state | conn: conn}}
        
      {:error, _reason} ->
        # Connection likely closed
        {:error, :connection_closed}
    end
  end
  
  @doc """
  Process a GraphQL query with incremental delivery over SSE.
  """
  def process_query(conn, schema, query, variables \\ %{}, options \\ []) do
    with {:ok, state} <- init(conn, options),
         {:ok, blueprint} <- parse_and_execute(query, schema, variables, options) do
      
      if incremental_delivery_enabled?(blueprint) do
        handle_streaming_response(state.conn, blueprint, options)
      else
        # Fallback to standard response
        send_standard_response(state, blueprint)
      end
    else
      {:error, reason} ->
        send_error_response(conn, reason)
    end
  end
  
  # Private functions
  
  defp accepts_sse?(conn) do
    case get_req_header(conn, "accept") do
      [] -> false
      headers ->
        Enum.any?(headers, fn header ->
          String.contains?(header, "text/event-stream") or
          String.contains?(header, "*/*")
        end)
    end
  end
  
  defp format_event(event_type, data, event_id) do
    encoded = Jason.encode!(data)
    
    [
      "id: #{event_id}\n",
      "event: #{event_type}\n",
      "data: #{encoded}\n",
      "\n"
    ]
    |> IO.iodata_to_binary()
  end
  
  defp format_error_response(error) when is_binary(error) do
    %{errors: [%{message: error}]}
  end
  
  defp format_error_response(error) when is_map(error) do
    %{errors: [error]}
  end
  
  defp format_error_response(errors) when is_list(errors) do
    %{errors: errors}
  end
  
  defp format_error_response(error) do
    %{errors: [%{message: inspect(error)}]}
  end
  
  defp schedule_keep_alive do
    Process.send_after(self(), :keep_alive, @keep_alive_interval)
  end
  
  defp parse_and_execute(query, schema, variables, options) do
    pipeline = 
      schema
      |> Absinthe.Pipeline.for_document(
        variables: variables,
        context: Map.get(options, :context, %{})
      )
      |> Absinthe.Pipeline.Incremental.enable(options)
    
    case Absinthe.Pipeline.run(query, pipeline) do
      {:ok, blueprint, _phases} ->
        {:ok, blueprint}
        
      {:error, msg, _phases} ->
        {:error, msg}
    end
  end
  
  defp incremental_delivery_enabled?(blueprint) do
    get_in(blueprint, [:execution, :incremental_delivery]) == true
  end
  
  defp send_standard_response(state, blueprint) do
    response = %{
      data: blueprint.result.data,
      errors: blueprint.result[:errors]
    }
    
    event_data = format_event("result", response, state.event_id)
    
    case chunk(state.conn, event_data) do
      {:ok, conn} ->
        # Close after sending
        chunk(conn, "")
        {:ok, conn}
        
      {:error, reason} ->
        {:error, reason}
    end
  end
  
  defp send_error_response(conn, reason) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(400, Jason.encode!(%{errors: [%{message: inspect(reason)}]}))
  end
end

defmodule Absinthe.Plug.Incremental.SSE.Router do
  @moduledoc """
  Plug router helper for SSE endpoints.
  
  This module provides macros to easily add SSE endpoints to your router.
  """
  
  defmacro sse_query(path, schema, opts \\ []) do
    quote do
      post unquote(path) do
        query = conn.body_params["query"] || conn.params["query"]
        variables = conn.body_params["variables"] || conn.params["variables"] || %{}
        
        Absinthe.Plug.Incremental.SSE.process_query(
          conn,
          unquote(schema),
          query,
          variables,
          unquote(opts)
        )
      end
      
      get unquote(path) do
        # Support GET requests for SSE
        query = conn.params["query"]
        variables = conn.params["variables"] || %{}
        
        if query do
          Absinthe.Plug.Incremental.SSE.process_query(
            conn,
            unquote(schema),
            query,
            variables,
            unquote(opts)
          )
        else
          conn
          |> put_resp_content_type("text/plain")
          |> send_resp(400, "Query parameter required")
        end
      end
    end
  end
end