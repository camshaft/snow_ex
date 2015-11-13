defmodule Snow.Enrich.Tag do
  def exec(stream, nil) do
    stream
  end
  def exec(stream, tags) do
    stream
    |> Stream.map(&(handle(&1, tags)))
  end

  defp handle(payload, tags) do
    Snow.Payload.put_event(payload, :etl_tags, tags)
  end
end
