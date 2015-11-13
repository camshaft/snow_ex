defmodule Snow.Enrich.Web.URL do
  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{page_url: url} = payload) when not is_nil(url) do
    Dict.put(payload, :derived_contexts, derive(url, payload))
  end
  defp handle(model) do
    model
  end

  defp derive(url = %{scheme: scheme, host: host, port: port, path: path, query: query, fragment: fragment}, %{page_title: page_title} = payload) do
    %Snow.Model.Context{
      parent: payload,
      schema: %{
        "vendor": "com.camshaft.snow.web",
        "name": "page_url",
        "format": "jsonschema",
        "version": "1-0-0"
      },
      data: %{
        "title" => page_title,
        "url" => to_string(url),
        "scheme" => scheme,
        "host" => host,
        "port" => port,
        "path" => path,
        "query" => query,
        "fragment" => fragment
      }
    }
  end
end
