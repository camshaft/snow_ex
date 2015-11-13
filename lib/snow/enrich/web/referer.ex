defmodule Snow.Enrich.Web.Referer do
  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{page_referrer: url} = payload) when not is_nil(url) do
    Dict.put(payload, :derived_contexts, derive(url, payload))
  end
  defp handle(payload) do
    payload
  end

  defp derive(url = %{scheme: scheme, host: host, port: port, path: path, query: query, fragment: fragment}, payload) do
    {medium, source, term} = parse(url, payload)

    %Snow.Model.Context{
      parent: payload,
      schema: %{
        "vendor": "com.camshaft.snow.web",
        "name": "referer",
        "format": "jsonschema",
        "version": "1-0-0"
      },
      data: %{
        "url" => to_string(url),
        "scheme" => scheme,
        "host" => host,
        "port" => port,
        "path" => path,
        "query" => query,
        "fragment" => fragment,
        "medium" => medium,
        "source" => source,
        "term" => term
      }
    }
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
