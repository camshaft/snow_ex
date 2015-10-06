defmodule Snow.Enricher.UserAgent do
  def exec(stream) do
    stream
    |> Stream.map(&handle/1)
  end

  defp handle(%{useragent: useragent} = model) when is_binary(useragent) do
    # case UAInspector.parse(useragent) do

    # end
    model
  end
  defp handle(model) do
    model
  end
end
