defmodule Snow.Enrich.Web.Marketing do
  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{page_url: %{query: query}} = payload) when not is_nil(query) do
    query
    |> URI.query_decoder()
    |> Enum.reduce(%{}, &parse/2)
    |> context(payload)
  catch
    _, _ ->
      payload
  end
  defp handle(payload) do
    payload
  end

  defp context(params, payload) do
    if Map.size(params) > 0 do
      Dict.put(payload, :derived_contexts, %Snow.Model.Context{
        parent: payload,
        schema: %{
          "vendor": "com.camshaft.snow.web",
          "name": "marketing",
          "format": "jsonschema",
          "version": "1-0-0"
        },
        data: params
      })
    else
      payload
    end
  end

  terms = [utm_medium: :medium,
           utm_source: :source,
           utm_term: :term,
           utm_content: :content,
           utm_campaign: :campaign]

  for {from, to} <- terms do
    defp parse({unquote(to_string(from)), value}, acc) do
      Map.put(acc, unquote(to_string(to)), value)
    end
  end
  defp parse(_, acc) do
    acc
  end
end
