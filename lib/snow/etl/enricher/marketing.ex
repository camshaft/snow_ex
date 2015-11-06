defmodule Snow.ETL.Enricher.Marketing do
  def exec(stream) do
    stream
    |> Stream.map(&handle/1)
  end

  defp handle(%{page_urlquery: query} = model) when is_binary(query) do
    query
    |> URI.query_decoder()
    |> Enum.reduce(model, &parse/2)
  catch
    _, _ ->
      model
  end
  defp handle(model) do
    model
  end

  terms = [utm_medium: :mkt_medium,
           utm_source: :mkt_source,
           utm_term: :mkt_term,
           utm_content: :mkt_content,
           utm_campaign: :mkt_campaign]

  for {from, to} <- terms do
    defp parse({unquote(to_string(from)), value}, acc) do
      %{acc | unquote(to) => value}
    end
  end
  defp parse(_, acc) do
    acc
  end
end
