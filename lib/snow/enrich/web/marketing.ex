defmodule Snow.Enrich.Web.Marketing do
  use Snow.Model.Context

  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{page_url: %{query: query}} = payload) when not is_nil(query) do
    query
    |> URI.query_decoder()
    |> Enum.reduce(nil, &parse/2)
    |> context(payload)
  end
  defp handle(payload) do
    payload
  end

  terms = [utm_medium: :medium,
           utm_source: :source,
           utm_term: :term,
           utm_content: :content,
           utm_campaign: :campaign]

  term_nils = terms |> Dict.values |> Enum.map(&({&1, nil}))
  term_vars = terms |> Dict.values |> Enum.map(&({&1, Macro.var(&1, nil)}))

  defp context(nil, payload) do
    payload
  end
  defp context(%{unquote_splicing(term_vars)}, payload) do
    put_context(payload, [vendor: "web", name: "marketing"], unquote(term_vars))
  end

  for {from, to} <- terms do
    defp parse({unquote(to_string(from)), value}, nil) do
      Map.put(empty, unquote(to), value)
    end
    defp parse({unquote(to_string(from)), value}, acc) do
      Map.put(acc, unquote(to), value)
    end
  end
  defp parse(_, acc) do
    acc
  end

  defp empty do
    %{unquote_splicing(term_nils)}
  end
end
