defmodule Snow.Enrich.Serializer.CSV do
  def exec(stream, paths, options \\ []) do
    stream
    |> Stream.map(fn(item) ->
      name = Snow.Model.name(item)
      data = [paths.match(name, item)] |> CSV.encode(options) |> Enum.to_list()
      {name, data}
    end)
  end
end
