defmodule Snow.ETL.Enricher.Referer do
  def exec(stream) do
    stream
    |> Stream.map(&handle/1)
  end

  defp handle(%{page_referrer: page_referrer} = model) when is_binary(page_referrer) do
    %{scheme: scheme,
      host: host,
      port: port,
      path: path,
      query: query,
      fragment: fragment} = refr = URI.parse(page_referrer)

    {medium, source, term} = parse(refr, model)

    %{model | refr_urlscheme: scheme,
              refr_urlhost: host,
              refr_urlport: port,
              refr_urlpath: path,
              refr_urlquery: query,
              refr_urlfragment: fragment,
              refr_medium: medium,
              refr_source: source,
              refr_term: term}
  end
  defp handle(model) do
    model
  end

  defp parse(%{host: host}, %{page_urlhost: host}) do
    {"internal", nil, nil}
  end
  defp parse(page_referrer, _) do
    case RefInspector.parse(page_referrer) do
      %{medium: medium, source: source, term: term} ->
        {to_string(medium), to_string(source), to_string(term)}
      _ ->
        {nil, nil, nil}
    end
  end
end
