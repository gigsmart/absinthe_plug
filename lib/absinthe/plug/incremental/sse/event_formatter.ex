defmodule Absinthe.Plug.Incremental.SSE.EventFormatter do
  @moduledoc """
  Handles formatting of SSE events for incremental delivery.
  
  This module is responsible for converting GraphQL responses into
  properly formatted SSE event data.
  """

  @doc """
  Format a GraphQL response as an SSE event.
  
  ## Parameters
  - `event_type` - The type of event (initial, incremental, complete, error)
  - `data` - The response data to include in the event
  - `event_id` - Unique identifier for this event
  
  ## Returns
  A binary string formatted as an SSE event.
  """
  @spec format_event(String.t(), map(), non_neg_integer()) :: binary()
  def format_event(event_type, data, event_id) do
    encoded = Jason.encode!(data)
    
    [
      "id: #{event_id}\n",
      "event: #{event_type}\n",
      "data: #{encoded}\n",
      "\n"
    ]
    |> IO.iodata_to_binary()
  end

  @doc """
  Format error data for SSE transmission.
  """
  @spec format_error_response(any()) :: map()
  def format_error_response(error) when is_binary(error) do
    %{errors: [%{message: error}]}
  end
  
  def format_error_response(error) when is_map(error) do
    %{errors: [error]}
  end
  
  def format_error_response(errors) when is_list(errors) do
    %{errors: errors}
  end
  
  def format_error_response(error) do
    %{errors: [%{message: inspect(error)}]}
  end
end