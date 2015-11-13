defmodule Snow.Payload.Generator do
  def payloads(_schemas \\ nil) do
    Stream.repeatedly(&payload/0)
  end

  def payload do
    required
    |> Stream.concat(optional)
    |> Snow.Payload.from_stream()
  end

  defp required do
    [
      v_tracker,
      platform,
      event,
      event_id
    ]
  end

  defp v_tracker do
    {"tv", "payload-generator-1"}
  end

  defp platform do
    {"p", one_of(unquote(Snow.Payload.platforms |> Enum.map(&(&1 |> elem(0) |> to_string))))}
  end

  defp event do
    {"e", one_of(unquote(Snow.Payload.events |> Enum.map(&(&1 |> elem(0) |> to_string))))}
  end

  defp event_id do
    {"eid", :crypto.rand_bytes(20) |> Base.encode16()}
  end

  defp optional do
    [{"url", one_of(["http://google.com?utm_medium=CoolBeans",
                     "https://github.com/camshaft/snow_ex"])},
     {"refr", one_of(["http://google.com?q=testing",
                      "https://amazon.com/?q=my+product"])},
     {"ip", "67.2.212.46"},
     {"ua", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36"},
     {"tv", "1.0.0"},]
  end

  defp one_of(list) do
    list
    |> Enum.shuffle()
    |> hd()
  end
end

