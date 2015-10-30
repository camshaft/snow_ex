# defmodule SnowTest.ETL do
#   use Snow.ETL
# end

defmodule SnowTest.Store do
  use Snow.Store, timeout: 5_000

  defp flush(ts, obj) do
    IO.inspect {ts, obj}
  end
end

defmodule SnowTest.Collector do
  use Snow.Collector, store: SnowTest.Store
end

defmodule SnowTest.Server do
  use Snow.Server, collector: SnowTest.Collector

  plug :match
  plug :dispatch

  match _ do
    send_resp(conn, 404, "")
  end
end

{:ok, _} = Plug.Adapters.Cowboy.http SnowTest.Server, []
