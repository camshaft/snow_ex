defmodule Snow.Enricher.Referer do
  def exec(stream) do
    stream
    |> Stream.map(&handle/1)
  end

  defp handle(%{page_referrer: page_referrer, page_url: page_url} = model) when is_binary(page_referrer) do
    {medium, source, term} = parse(page_referrer, page_url)

    %{scheme: scheme,
      host: host,
      port: port,
      path: path,
      query: query,
      fragment: fragment} = URI.parse(page_referrer)
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

  defp parse(page_referrer, page_url) do
    ## TODO
    {nil, nil, nil}
  end
end
