defmodule Snow.Parser.Cloudfront do
  def exec(stream) do
    Stream.resource(
      fn ->
        {stream, []}
      end,
      &handle/1,
      fn(_) ->
        :ok
      end
    )
  end

  defp next(nil) do
    {:done, nil}
  end
  defp next(reducer) when is_function(reducer) do
    reducer.({:cont, []})
  end
  defp next(stream) do
    Enumerable.reduce(stream, {:cont, []}, fn(value, _) -> {:suspend, value} end)
  end

  defp handle({stream, buffer}) do
    case next(stream) do
      {status, _} when status in [:done, :halted] ->
        {:halt, stream}
      {:suspended, value, stream} ->
        {events, buffer} = parse(to_string([buffer, value]), [])
        {events, {stream, buffer}}
    end
  end

  def parse(buffer, acc) do
    {model, buffer} = parse_line(buffer)
    parse(buffer, [model | acc])
  catch
    :cont ->
      {:lists.reverse(acc), buffer}
    {:pass, buffer} ->
      parse(buffer, acc)
  end

  defp parse_line(b) do
    {date, b} = consume_tab(b)
    {time, b} = consume_tab(b)
    {x_edge_location, b} = consume_tab(b)
    {_sc_bytes, b} = consume_tab(b)
    {c_ip, b} = consume_tab(b)
    {_cs_method, b} = consume_tab(b)
    {_cs_host, b} = consume_tab(b)
    {cs_uri_stem, b} = consume_tab(b)
    {_sc_status, b} = consume_tab(b)
    {cs_referer, b} = consume_tab(b)
    {cs_user_agent, b} = consume_tab(b)
    {cs_uri_query, b} = consume_tab(b)
    {_cs_cookie, b} = consume_tab(b)
    {_x_edge_result_type, b} = consume_tab(b)
    {x_edge_request_id, b} = consume_tab(b)
    {_x_host_header, b} = consume_tab(b)
    {_cs_protocol, b} = consume_tab(b)
    {_cs_bytes, b} = consume_tab(b)
    {_time_taken, b} = consume_tab(b)
    {_x_forwarded_for, b} = consume_tab(b)
    {_ssl_protocol, b} = consume_tab(b)
    {_ssl_cipher, b} = consume_tab(b)
    {_x_edge_response_result_type, b} = consume_nl(b)

    cs_uri_stem !== "/i" && throw {:pass, b}

    model = cs_uri_query
    |> Snow.Model.from_string()
    |> Map.put_new(:event_id, x_edge_location <> "|" <> x_edge_request_id)
    |> Map.put_new(:page_url, cs_referer)
    |> Map.put_new(:useragent, cs_user_agent)
    |> Map.merge(%{v_collector: "cf",
                   collector_tstamp: 0, ## TODO
                   user_ipaddress: c_ip})

    {model, b}
  end

  for i <- 1..5_000 do
    def consume_tab(<<value :: size(unquote(i))-binary, "\t", rest :: binary>>) do
      {URI.decode(value), rest}
    end
    def consume_nl(<<value :: size(unquote(i))-binary, "\n", rest :: binary>>) do
      {URI.decode(value), rest}
    end
  end
  def consume_tab(_) do
    throw :cont
  end
  def consume_nl(_) do
    throw :cont
  end
end
