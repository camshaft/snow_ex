defmodule Snow.ETL.Enricher.URL do
  def exec(stream) do
    stream
    |> Stream.map(&handle/1)
  end

  defp handle(%{page_url: url} = model) when is_binary(url) do
    %{scheme: scheme,
      host: host,
      port: port,
      path: path,
      query: query,
      fragment: fragment} = URI.parse(url)
    %{model | page_urlscheme: scheme,
              page_urlhost: host,
              page_urlport: port,
              page_urlpath: path,
              page_urlquery: query,
              page_urlfragment: fragment}
  end
  defp handle(model) do
    model
  end
end
