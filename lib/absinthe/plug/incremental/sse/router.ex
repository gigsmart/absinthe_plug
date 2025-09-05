defmodule Absinthe.Plug.Incremental.SSE.Router do
  @moduledoc """
  Plug router helpers for SSE endpoints.
  
  This module provides convenient macros and plugs for adding
  GraphQL SSE endpoints to your Phoenix or Plug router.
  
  ## Usage
  
      # In your Phoenix router
      import Absinthe.Plug.Incremental.SSE.Router
      
      pipeline :graphql_streaming do
        plug :accepts, ["json"]
        plug Absinthe.Plug.Incremental.SSE.Plug
      end
      
      scope "/api" do
        pipe_through :graphql_streaming
        
        sse_query "/graphql/stream", MyApp.Schema
      end
  
  ## JavaScript Client Example
  
      const eventSource = new EventSource('/api/graphql/stream?' + 
        new URLSearchParams({
          query: `
            query GetUsers {
              users @stream(initialCount: 2, label: "users") {
                id
                name
              }
            }
          `
        }));
      
      eventSource.addEventListener('initial', (event) => {
        const data = JSON.parse(event.data);
        console.log('Initial data:', data);
      });
      
      eventSource.addEventListener('incremental', (event) => {
        const data = JSON.parse(event.data);
        console.log('Incremental data:', data);
      });
      
      eventSource.addEventListener('complete', (event) => {
        console.log('Streaming complete');
        eventSource.close();
      });
  """

  import Plug.Conn

  @doc """
  Macro for creating SSE GraphQL endpoints.
  
  ## Parameters
  - `path` - The URL path for the endpoint
  - `schema` - The Absinthe schema module
  - `opts` - Additional options (optional)
  
  ## Options
  - `:context` - Additional context for query execution
  - `:operation_id` - Custom operation ID generator
  - `:keep_alive` - Enable keep-alive messages (default: true)
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

defmodule Absinthe.Plug.Incremental.SSE.Plug do
  @moduledoc """
  Plug for SSE-specific middleware.
  
  This plug sets up the necessary middleware for SSE streaming,
  including proper CORS headers and connection handling.
  """

  @behaviour Plug

  import Plug.Conn

  def init(opts), do: opts

  def call(conn, _opts) do
    conn
    |> put_resp_header("access-control-allow-origin", "*")
    |> put_resp_header("access-control-allow-headers", "content-type, cache-control")
    |> put_resp_header("access-control-allow-methods", "GET, POST, OPTIONS")
  end
end