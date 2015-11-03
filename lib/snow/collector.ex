defmodule Snow.Collector do
  name = Mix.Project.config()[:app]
  version = Mix.Project.config()[:version]

  defmacro __using__(opts) do
    store = opts[:store]
    vc =  unquote("#{name}-#{version}")

    quote do
      def collect(qs, ip, ua) do
        init = %{"ua" => ua,
                 "ip" => Snow.Collector.format_ip(ip)}

        qs
        |> URI.query_decoder()
        |> Enum.reduce(init, fn({k, v}, acc) ->
          Map.put(acc, k, v)
        end)
        |> Map.merge(%{
          "$cv" => unquote(vc),
          "$ct" => Snow.Collector.timestamp
        })
        |> encode()
        |> unquote(store).store()
      end

      def encode(event) do
        event
        |> Msgpax.pack!()
        |> :erlang.iolist_to_binary()
      end

      defoverridable [encode: 1]
    end
  end

  def timestamp do
    {mega, sec, microsec} = :os.timestamp()
    mega * 1_000_000_000 + sec * 1_000 + div(microsec, 1_000)
  end

  def format_ip({a,b,c,d}) do
    "#{a}.#{b}.#{c}.#{d}"
  end
  def format_ip({a,b,c,d,e,f,g,h}) do
    to_hex(a) <> ":" <>
      to_hex(b) <> ":" <>
      to_hex(c) <> ":" <>
      to_hex(d) <> ":" <>
      to_hex(e) <> ":" <>
      to_hex(f) <> ":" <>
      to_hex(g) <> ":" <>
      to_hex(h) <> ":"
  end
  def format_ip(nil) do
    nil
  end

  defp to_hex(num) do
    :erlang.integer_to_binary(num, 16)
  end
end
