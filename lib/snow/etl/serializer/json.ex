defmodule Snow.ETL.Serializer.JSON do
  def exec(stream, options \\ []) do
    stream
    |> Stream.map(fn(item) ->
      {Snow.ETL.Shredder.Utils.name(item), [Poison.encode_to_iodata!(item, options), "\n"]}
    end)
  end
end
