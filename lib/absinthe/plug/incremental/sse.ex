defmodule Absinthe.Plug.Incremental.SSE do
  @moduledoc """
  Server-Sent Events (SSE) transport for incremental delivery.
  
  This module implements incremental delivery over HTTP using SSE,
  allowing @defer and @stream directives to work over standard HTTP connections.
  
  ## Usage
  
      # In your Phoenix router
      pipeline :graphql_streaming do
        plug Absinthe.Plug.Incremental.SSE
      end
      
      scope "/api" do
        pipe_through :graphql_streaming
        
        post "/graphql/stream", GraphQLController, :stream_query
      end
  
  ## Example Query with Streaming
  
      query GetUsers {
        users @stream(initialCount: 2, label: "users") {
          id
          name
          ... @defer(label: "profile") {
            profile {
              bio
              avatar
            }
          }
        }
      }
  
  The response will be delivered as a series of SSE events:
  - `initial` event with the first 2 users
  - `incremental` events for remaining users and deferred profiles
  - `complete` event when all data is delivered
  """
  
  use Absinthe.Incremental.Transport
  import Plug.Conn
  
  require Logger
  
  alias Absinthe.Plug.Incremental.SSE.{EventFormatter, ConnectionManager, QueryProcessor}
  
  @content_type "text/event-stream"
  @keep_alive_interval 30_000  # 30 seconds
  
  @impl true
  def init(conn, options) do
    if ConnectionManager.accepts_sse?(conn) do
      conn = ConnectionManager.setup_sse_headers(conn)
      
      if Keyword.get(options, :keep_alive, true) do
        ConnectionManager.schedule_keep_alive()
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
    event_data = EventFormatter.format_event("initial", response, state.event_id)
    
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
    event_data = EventFormatter.format_event("incremental", response, state.event_id)
    
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
    event_data = EventFormatter.format_event("complete", %{}, state.event_id)
    
    case chunk(state.conn, event_data) do
      {:ok, conn} ->
        chunk(conn, "")
        :ok
        
      {:error, reason} ->
        Logger.error("Failed to send complete SSE event: #{inspect(reason)}")
        {:error, {:transport_error, reason}}
    end
  end
  
  @impl true
  def handle_error(state, error) do
    error_response = EventFormatter.format_error_response(error)
    event_data = EventFormatter.format_event("error", error_response, state.event_id)
    
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
    case chunk(state.conn, ": keep-alive\n\n") do
      {:ok, conn} ->
        ConnectionManager.schedule_keep_alive()
        {:ok, %{state | conn: conn}}
        
      {:error, _reason} ->
        {:error, :connection_closed}
    end
  end
  
  @doc """
  Process a GraphQL query with incremental delivery over SSE.
  """
  def process_query(conn, schema, query, variables \\ %{}, options \\ []) do
    QueryProcessor.process(conn, schema, query, variables, options)
  end
end
