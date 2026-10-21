defmodule Absinthe.Plug.IncrementalTest do
  use Absinthe.Plug.TestCase

  alias Absinthe.Plug.TestSchema

  @query ~S|{ item(id: "foo") { id ... @defer(label: "n") { name } } }|
  @multipart ~s(multipart/mixed; boundary="-"; deferSpec=20220824)

  defp request(body, opts, accept \\ "multipart/mixed") do
    conn =
      conn(:post, "/", body)
      |> put_req_header("content-type", "application/json")

    conn = if accept, do: put_req_header(conn, "accept", accept), else: conn

    conn
    |> plug_parser()
    |> Absinthe.Plug.call(Absinthe.Plug.init([schema: TestSchema] ++ opts))
  end

  defp parts(body) do
    ["" | parts] = String.split(body, "\r\n---")
    "--\r\n" = List.last(parts)

    parts
    |> Enum.drop(-1)
    |> Enum.map(fn part ->
      [_headers, json] = String.split(part, "\r\n\r\n", parts: 2)
      Jason.decode!(json)
    end)
  end

  test "a deferring request that accepts multipart/mixed is streamed" do
    conn = request(Jason.encode!(%{query: @query}), incremental: true)

    assert conn.status == 200
    assert get_resp_header(conn, "content-type") == [@multipart]
    assert String.starts_with?(conn.resp_body, "\r\n---\r\n")
    assert String.ends_with?(conn.resp_body, "\r\n-----\r\n")

    assert parts(conn.resp_body) == [
             %{"data" => %{"item" => %{"id" => "foo"}}, "hasNext" => true},
             %{
               "incremental" => [
                 %{"data" => %{"name" => "Foo"}, "path" => ["item"], "label" => "n"}
               ],
               "hasNext" => false
             }
           ]
  end

  test "without a multipart accept header deferred fields resolve inline" do
    conn = request(Jason.encode!(%{query: @query}), [incremental: true], nil)

    assert conn.status == 200

    assert Jason.decode!(conn.resp_body) == %{
             "data" => %{"item" => %{"id" => "foo", "name" => "Foo"}}
           }
  end

  test "with incremental disabled deferred fields resolve inline" do
    conn = request(Jason.encode!(%{query: @query}), incremental: false)

    assert conn.status == 200

    assert Jason.decode!(conn.resp_body) == %{
             "data" => %{"item" => %{"id" => "foo", "name" => "Foo"}}
           }
  end

  test "transport-batched requests resolve deferred fields inline" do
    conn = request(Jason.encode!([%{query: @query}]), incremental: true)

    assert conn.status == 200

    assert Jason.decode!(conn.resp_body) == [
             %{"payload" => %{"data" => %{"item" => %{"id" => "foo", "name" => "Foo"}}}}
           ]
  end

  test "a document without @defer is answered as JSON" do
    conn =
      request(Jason.encode!(%{query: ~S|{ item(id: "foo") { id name } }|}), incremental: true)

    assert conn.status == 200
    assert [content_type] = get_resp_header(conn, "content-type")
    assert String.starts_with?(content_type, "application/json")

    assert Jason.decode!(conn.resp_body) == %{
             "data" => %{"item" => %{"id" => "foo", "name" => "Foo"}}
           }
  end
end
