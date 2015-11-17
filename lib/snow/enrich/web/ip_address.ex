defmodule Snow.Enrich.Web.IPAddress do
  use Snow.Model.Context

  def exec(stream) do
    stream
    |> Stream.map(&handle/1)
  end

  defp handle(%{user_ipaddress: ip} = payload) when not is_nil(ip) do
    ip
    |> Geolix.lookup(locale: :en)
    |> extract(payload)
    |> put_context([vendor: "web", name: "ip_address"], [
      ip_address: ip :: string
    ])
  end
  defp handle(payload) do
    payload
  end

  types = [city: [:name, {:integer, :geoname_id}],
           continent: [:name, {:integer, :geoname_id}, :code],
           country: [:name, {:integer, :geoname_id}, :iso_code],
           location: [{:number, :latitude}, {:number, :longitude}, {:integer, :metro_code}, :time_zone],
           postal: [:code],
           subdivisions: [:name, {:integer, :geoname_id}, :iso_code]]

  defp extract(info, payload) do
    unquote(Enum.reduce(types, Macro.var(:payload, nil), fn({name, _}, acc) ->
      {:"extract_#{name}", [], [Macro.var(:info, nil), acc]}
    end))
  end

  for {name, params} <- types do
    fn_name = :"extract_#{name}"

    args = params |> Enum.map(fn
      {_, name} ->
        {name, Macro.var(name, nil)}
      name ->
        {name, Macro.var(name, nil)}
    end)

    data = params |> Enum.map(fn
      ({type, name}) ->
        {name, {:::, [], [Macro.var(name, nil), type]}}
      (name) ->
        {name, {:::, [], [Macro.var(name, nil), :string]}}
    end)

    for section <- [:city, :country] do
      defp unquote(fn_name)(%{unquote(section) => %{unquote(name) => results}}, payload) when is_list(results) do
        Enum.reduce(results, payload, fn(result, payload) ->
          unquote(fn_name)(%{unquote(section) => %{unquote(name) => result}}, payload)
        end)
      end
      defp unquote(fn_name)(%{unquote(section) => %{unquote(name) => %{unquote_splicing(args)}}}, payload) do
        put_context(payload, [vendor: "web.geo", name: unquote(name)], unquote(data))
      end
    end
    defp unquote(fn_name)(_, payload) do
      payload
    end
  end
end
