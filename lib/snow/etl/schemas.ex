defmodule Snow.ETL.Schemas do
  defmacro __using__(opts) do
    url = opts[:index]
    rate = opts[:refresh_rate] || 60_000
    quote do
      use GenServer

      def start_link(init, opts) do
        GenServer.start_link(__MODULE__, init, opts)
      end

      def init(_) do
        load(:infinity)
        Process.send_after(self(), :reload, unquote(rate))
        {:ok, nil}
      end

      def handle_info(:reload, s) do
        load(5)
        Process.send_after(self(), :reload, unquote(rate))
        {:noreply, s}
      end

      def load(concurrency) do
        unquote(url)
        |> Snow.Utils.get_json!()
        |> Nile.pmap(&Snow.Utils.get_json!/1, [concurrency: concurrency])
        |> Enum.to_list()
        |> Snow.ETL.Schemas.Compiler.define(__MODULE__.Schemas)
      end

      def shred(event, parent) do
        __MODULE__.Schemas.shred(event, parent)
      end
    end
  end
end
