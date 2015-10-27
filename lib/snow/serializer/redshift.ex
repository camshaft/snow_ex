defmodule Snow.Serializer.Redshift do
  def exec(stream, options \\ []) do
    stream
    |> Stream.map(&(Poison.encode!(&1, options) <> "\n"))
  end
end