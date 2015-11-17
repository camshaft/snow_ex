defmodule Snow.Payload.Generator do
  def payloads(_schemas \\ nil) do
    Stream.repeatedly(&payload/0)
  end

  def payload do
    required
    |> Stream.concat(optional)
    |> Snow.Payload.from_dict()
  end

  defp required do
    [
      v_tracker,
      platform,
      event,
      event_id,
      collector_tstamp
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

  defp collector_tstamp do
    {"$ct", Snow.Utils.timestamp}
  end

  defp optional do
    [{"url", one_of(["http://google.com?utm_medium=CoolBeans",
                     "https://github.com/camshaft/snow_ex"])},
     {"refr", one_of(["http://google.com?q=testing",
                      "https://amazon.com/?q=my+product"])},
     {"ip", "67.2.212.46"},
     {"ua", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36"},
     {"tv", "1.0.0"},
     {"aid", one_of(["app_1", "app_2", "app_3"])},
     {"res", "200x300"},
     {"f_pdf", tracker_bool},
     {"f_qt", tracker_bool},
     {"f_realp", tracker_bool},
     {"f_wma", tracker_bool},
     {"f_dir", tracker_bool},
     {"f_fla", tracker_bool},
     {"f_java", tracker_bool},
     {"f_gears", tracker_bool},
     {"f_ag", tracker_bool}]
  end

  defp tracker_bool do
    one_of(["0", "1", nil])
  end

  defp one_of(list) do
    list
    |> Enum.shuffle()
    |> hd()
  end
end
