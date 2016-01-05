defmodule Snow.Enrich.Serializer.CSV do
  def exec(stream, paths) do
    stream
    |> Stream.map(fn(item) ->
      name = Snow.Model.name(item)
      data = paths.match(name, item) |> Enum.join(",")
      {name, [data, "\r\n"]}
    end)
  end
end
