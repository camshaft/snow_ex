defmodule Snow.Enrich.Common.User do
  use Snow.Model.Context

  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{user_id: nil, domain_userid: nil, network_userid: nil} = payload) do
    payload
  end
  defp handle(%{user_id: user_id, domain_userid: domain_userid, network_userid: network_userid} = payload) do
    put_context(payload, [name: "user"], [
      id: user_id :: string,
      domain_id: domain_userid :: string,
      network_id: network_userid :: string
    ])
  end
end
