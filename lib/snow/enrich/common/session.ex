defmodule Snow.Enrich.Common.Session do
  use Snow.Model.Context

  def exec(stream) do
    Stream.map(stream, &handle/1)
  end

  defp handle(%{domain_sessionidx: nil, domain_sessionid: nil} = payload) do
    payload
  end
  defp handle(%{domain_sessionidx: domain_sessionidx, domain_sessionid: domain_sessionid} = payload) do
    put_context(payload, [name: "session"], [
      id: domain_sessionid :: string,
      index: domain_sessionidx :: integer
    ])
  end
end
