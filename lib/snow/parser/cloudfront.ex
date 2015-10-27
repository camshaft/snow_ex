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
  defp next({:cont, reducer}) when is_function(reducer) do
    {:cont, []}
    |> reducer.()
    |> wrap_cont()
  end
  defp next(reducer) when is_function(reducer) do
    {:cont, []}
    |> reducer.(fn(value, _) -> {:suspend, value} end)
    |> wrap_cont()
  end
  defp next(stream) do
    stream
    |> Enumerable.reduce({:cont, []}, fn(value, _) -> {:suspend, value} end)
    |> wrap_cont()
  end

  defp wrap_cont({:suspended, value, stream}) do
    {:suspended, value, {:cont, stream}}
  end
  defp wrap_cont(other) do
    other
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

  defp parse_line(<<"#", b :: binary>>) do
    {_, b} = consume_nl(b)
    throw {:pass, b}
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
    |> put_new(:event_id, x_edge_location <> "|" <> x_edge_request_id)
    |> put_new(:page_url, cs_referer)
    |> put_new(:useragent, URI.decode(cs_user_agent))
    |> Map.merge(%{v_collector: "cf",
                   collector_tstamp: parse_tstamp(date, time),
                   user_ipaddress: c_ip})

    {model, b}
  end

  defp put_new(map, key, value) do
    case Map.get(map, key) do
      nil ->
        Map.put(map, key, value)
      _ ->
        map
    end
  end

  def parse_tstamp(date, time) do
    time = {parse_date(date), parse_time(time)}
    |> :calendar.datetime_to_gregorian_seconds()
    |> - 62167219200
    time * 1000
  end

  defp parse_date(<<year :: size(4)-binary, "-", month :: size(2)-binary, "-", day :: size(2)-binary>>) do
    {String.to_integer(year),
     String.to_integer(month),
     String.to_integer(day)}
  end

  defp parse_time(<<hour :: size(2)-binary, ":", minute :: size(2)-binary, ":", second :: size(2)-binary>>) do
    {String.to_integer(hour),
     String.to_integer(minute),
     String.to_integer(second)}
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
