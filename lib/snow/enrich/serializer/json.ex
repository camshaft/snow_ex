defmodule Snow.Enrich.Serializer.JSON do
  def exec(stream, options \\ []) do
    stream
    |> Stream.map(fn(item) ->
      {Snow.Model.name(item), [Poison.encode_to_iodata!(item, options), "\n"]}
    end)
  end
end
