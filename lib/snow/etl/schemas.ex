defmodule Snow.ETL.Schemas do
  defmacro __using__(opts) do
    url = opts[:index]
    quote do
      use GenServer

      def start_link(init, opts) do
        GenServer.start_link(__MODULE__, init, opts)
      end

      def init(_) do
        load(:infinity)
        Process.send_after(self(), :reload, 60_000)
        {:ok, nil}
      end

      def handle_info(:reload, s) do
        load(5)
        {:noreply, s}
      end

      def load(concurrency) do
        unquote(url)
        |> Snow.ETL.Schemas.get_json!()
        |> Nile.pmap(&Snow.ETL.Schemas.get_json!/1, [concurrency: concurrency])
        |> Enum.to_list()
        |> Snow.ETL.Schemas.define(__MODULE__.Schemas)
      end

      def schemas do
        struct(__MODULE__.Schemas, [])
      end
    end
  end

  def get_json!(url) do
    %{body: body, status_code: 200} = HTTPoison.get!(url)
    body
    |> Poison.decode!()
  end

  def define(schemas, mod) do
    quote do
      defmodule unquote(mod) do
        defstruct __info__: nil

        def list do
          unquote(Macro.escape(schemas))
        end

        def get(model, key, default) do
          case fetch(model, key) do
            :error ->
              default
            {:ok, value} ->
              value
          end
        end

        unquote(Enum.map(schemas, &format/1))
        def fetch(_, _), do: :error
      end
    end
    |> Snow.Utils.eval_quoted()
  end

  defp format(schema = %{"$schema" => long,
                         "self" => %{"vendor" => vendor,
                                     "name" => name,
                                     "format" => format,
                                     "version" => version}}) do
    short = "#{vendor}/#{name}/#{format}/#{version}"
    short_w_iglu = "iglu:#{vendor}/#{name}/#{format}/#{version}"
    formats = [short, short_w_iglu, long]
    quote do
      def fetch(_, s) when s in unquote(formats) do
        {:ok, unquote(Macro.escape(schema))}
      end
    end
  end
end
