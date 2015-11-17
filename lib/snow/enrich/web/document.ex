defmodule Snow.Enrich.Web.Document do
  use Snow.Model.Context

  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{page_url: url} = payload) when not is_nil(url) do
    derive(url, payload)
  end
  defp handle(model) do
    model
  end

  defp derive(%{scheme: scheme, host: host, port: port, path: path, query: query, fragment: fragment} = url,
              %{page_title: page_title, doc_charset: charset, doc_size: {width, height}} = payload) do
    put_context(payload, [vendor: "web", name: "document"], [
      title: page_title :: string,
      url: to_string(url) :: string,
      scheme: scheme :: string,
      host: host :: string,
      port: port :: integer,
      path: path :: string,
      query: query :: string,
      fragment: fragment :: string,
      width: width :: integer,
      height: height :: integer,
      charset: charset :: string
    ])
  end
end
