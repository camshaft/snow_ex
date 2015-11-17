defmodule Snow.Enrich.Web.Referer do
  use Snow.Model.Context

  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{page_referrer: url} = payload) when not is_nil(url) do
    derive(url, payload)
  end
  defp handle(payload) do
    payload
  end

  defp derive(url = %{scheme: scheme, host: host, port: port, path: path, query: query, fragment: fragment}, payload) do
    {medium, source, term} = parse(url, payload)

    put_context(payload, [vendor: "web", name: "referer"], [
      url: to_string(url) :: string,
      scheme: scheme :: string,
      host: host :: string,
      port: port :: integer,
      path: path :: string,
      query: query :: string,
      fragment: fragment :: string,
      medium: medium :: string,
      source: source :: string,
      term: term :: string
    ])
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
