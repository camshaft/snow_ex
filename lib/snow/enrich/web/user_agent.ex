defmodule Snow.Enrich.Web.UserAgent do
  use Snow.Model.Context

  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{useragent: useragent} = payload) when is_binary(useragent) do
    useragent
    |> UAInspector.parse_client()
    |> extract(payload)
    |> put_context([vendor: "web", name: "useragent"], [useragent: useragent])
  end
  defp handle(payload) do
    payload
  end

  types = [client: [:engine, :name, :type, :version],
           device: [:brand, :model, :type],
           os: [:name, :platform, :version]]

  defp extract(info, payload) do
    unquote(Enum.reduce(types, Macro.var(:payload, nil), fn({name, _}, acc) ->
      {:"extract_#{name}", [], [Macro.var(:info, nil), acc]}
    end))
  end

  for {name, params} <- types do
    fn_name = :"extract_#{name}"

    args_unknown = {:%{}, [], params |> Enum.map(&({&1, :unknown}))}
    args = {:%{}, [], params |> Enum.map(&({&1, Macro.var(&1, nil)}))}
    data = params |> Enum.map(&({&1, Macro.var(&1, nil)}))

    defp unquote(fn_name)(%{unquote(name) => unquote(args_unknown)}, payload) do
      payload
    end
    defp unquote(fn_name)(%{unquote(name) => unquote(args)}, payload) do
      put_context(payload, [vendor: "web.useragent", name: unquote(name)], unquote(data))
    end
    defp unquote(fn_name)(_, payload) do
      payload
    end
  end
end
