defmodule Snow.Enrich.Context do
  @context "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1"

  def exec(stream, schemas \\ %{}) do
    stream |> Snow.Payload.derive(&(shred(&1, schemas)))
  end

  defp shred(%{context: nil}, _) do
    []
  end
  defp shred(model = %{context: %{"schema" => @context <> _, "data" => contexts}}, schemas) do
    shred(%{model | context: contexts}, schemas)
  end
  defp shred(model = %{context: contexts}, schemas) when is_list(contexts) do
    Enum.reduce(contexts, [], fn(event, acc) ->
      schemas.shred(event, model) ++ acc
    end)
  end
end
