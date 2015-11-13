defmodule Snow.Enrich.Web.Session do
  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{user_id: user_id, domain_userid: domain_userid, network_userid: network_userid,
                domain_sessionidx: domain_sessionidx, domain_sessionid: domain_sessionid} = payload) do
    Dict.put(payload, :derived_contexts, %Snow.Model.Context{
      parent: payload,
      schema: %{
        "vendor": "com.camshaft.snow.web",
        "name": "session",
        "format": "jsonschema",
        "version": "1-0-0"
      },
      data: %{

      }
    })
  end
  defp handle(model) do
    model
  end
end
