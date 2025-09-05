defmodule Absinthe.Plug.Incremental.SSE.QueryProcessor do
  @moduledoc """
  Handles GraphQL query processing for SSE transport.
  
  This module manages the execution of GraphQL queries and coordinates
  the streaming of responses over SSE.
  """

  import Plug.Conn
  require Logger

  alias Absinthe.Plug.Incremental.SSE.EventFormatter

  @doc """
  Process a GraphQL query with SSE streaming support.
  """
  @spec process(Plug.Conn.t(), module(), String.t(), map(), keyword()) :: Plug.Conn.t()
  def process(conn, schema, query, variables \\ %{}, options \\ []) do
    with {:ok, state} <- init_state(conn, options),
         {:ok, blueprint} <- parse_and_execute(query, schema, variables, options) do
      
      if incremental_delivery_enabled?(blueprint) do
        handle_streaming_response(state, blueprint, options)
      else
        send_standard_response(state, blueprint)
      end
    else
      {:error, reason} ->
        send_error_response(conn, reason)
    end
  end

  defp init_state(conn, options) do
    {:ok, %{
      conn: conn,
      operation_id: Keyword.get(options, :operation_id),
      event_id: 0,
      options: options
    }}
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

  defp handle_streaming_response(state, blueprint, _options) do
    # This would be handled by the transport layer
    # For now, we'll simulate the streaming behavior
    send_standard_response(state, blueprint)
  end

  defp send_standard_response(state, blueprint) do
    response = %{
      data: blueprint.result.data,
      errors: blueprint.result[:errors]
    }
    
    event_data = EventFormatter.format_event("result", response, state.event_id)
    
    case chunk(state.conn, event_data) do
      {:ok, conn} ->
        # Close after sending
        chunk(conn, "")
        conn
        
      {:error, reason} ->
        Logger.error("Failed to send SSE response: #{inspect(reason)}")
        state.conn
        |> put_resp_content_type("application/json")
        |> send_resp(500, Jason.encode!(%{errors: [%{message: "Transport error"}]}))
    end
  end

  defp send_error_response(conn, reason) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(400, Jason.encode!(%{errors: [%{message: inspect(reason)}]}))
  end
end