defmodule Snow.Enrich.Common.Device do
  use Snow.Model.Context

  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{device_resolution: {nil, nil}} = payload) do
    payload
  end
  defp handle(%{device_resolution: {width, height}} = payload) do
    put_context(payload, [name: "device"], [
      resolution_height: height :: integer,
      resolution_width: width :: integer
    ])
  end
end
