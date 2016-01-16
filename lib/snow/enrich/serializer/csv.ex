defmodule Snow.Enrich.Serializer.CSV do
  require Logger

  def exec(stream, paths) do
    stream
    |> Nile.expand(fn(item) ->
      try do
        name = Snow.Model.name(item)
        data = paths.match(name, item) |> Enum.join(",")
        [{name, [data, "\r\n"]}]
      rescue
        UnicodeConversionError ->
          Logger.error("UnicodeConversionError for #{inspect(item, limit: :infinity)}")
          []
      end
    end)
  end
end
