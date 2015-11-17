defmodule Snow.Collector do
  name = Mix.Project.config()[:app]
  version = Mix.Project.config()[:version]

  defmacro __using__(opts) do
    store = opts[:store]
    vc =  unquote("#{name}-#{version}")

    quote do
      def collect(qs, ip, ua, event_id) do
        init = %{"ua" => ua,
                 "ip" => Snow.Utils.format_ip(ip),
                 "eid" => event_id}

        qs
        |> URI.query_decoder()
        |> Enum.reduce(init, fn({k, v}, acc) ->
          Map.put(acc, k, v)
        end)
        |> Map.merge(%{
          "$cv" => unquote(vc),
          "$ct" => Snow.Utils.timestamp
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
end
