defmodule Snow.ETL.Parser.Cloudfront do
  alias Snow.ETL.Parser.Cloudfront.Lexer

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

  defp handle({stream, buffer}) do
    case Nile.Utils.next(stream) do
      {status, _} when status in [:done, :halted] ->
        {:halt, stream}
      {:suspended, value, stream} ->
        {events, buffer} = parse(to_string([buffer, value]), [])
        {events, {stream, buffer}}
    end
  end

  def parse(buffer, acc) do
    {model, buffer} = buffer |> parse_line()
    parse(buffer, [model | acc])
  catch
    :cont ->
      {:lists.reverse(acc), buffer}
    {:pass, buffer} ->
      parse(buffer, acc)
  end

  defp parse_line(<<"#", b :: binary>>) do
    {_, b} = Lexer.consume_nl(b)
    throw {:pass, b}
  end
  defp parse_line(line) do
    b = decode(line)
    {date, b} = Lexer.consume_tab(b)
    {time, b} = Lexer.consume_tab(b)
    {x_edge_location, b} = Lexer.consume_tab(b)
    {_sc_bytes, b} = Lexer.consume_tab(b)
    {c_ip, b} = Lexer.consume_tab(b)
    {_cs_method, b} = Lexer.consume_tab(b)
    {_cs_host, b} = Lexer.consume_tab(b)
    {cs_uri_stem, b} = Lexer.consume_tab(b)
    {_sc_status, b} = Lexer.consume_tab(b)
    {cs_referer, b} = Lexer.consume_tab(b)
    {cs_user_agent, b} = Lexer.consume_tab(b)
    {cs_uri_query, b} = Lexer.consume_tab(b)
    {_cs_cookie, b} = Lexer.consume_tab(b)
    {_x_edge_result_type, b} = Lexer.consume_tab(b)
    {x_edge_request_id, b} = Lexer.consume_tab(b)
    {_x_host_header, b} = Lexer.consume_tab(b)
    {_cs_protocol, b} = Lexer.consume_tab(b)
    {_cs_bytes, b} = Lexer.consume_tab(b)
    {_time_taken, b} = Lexer.consume_tab(b)
    {_x_forwarded_for, b} = Lexer.consume_tab(b)
    {_ssl_protocol, b} = Lexer.consume_tab(b)
    {_ssl_cipher, b} = Lexer.consume_tab(b)
    {_x_edge_response_result_type, b} = Lexer.consume_nl(b)

    cs_uri_stem !== "/i" && throw {:pass, b}

    model = cs_uri_query
    |> Snow.Model.from_string()
    |> put_new(:event_id, x_edge_location <> "|" <> x_edge_request_id)
    |> put_new(:page_url, cs_referer)
    |> put_new(:useragent, decode(cs_user_agent))
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

  defp decode(value) do
    URI.decode(value)
  rescue
    _e ->
      value
  end
end
