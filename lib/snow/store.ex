defmodule Snow.Store do
  defmacro __using__(opts) do
    quote do
      Snow.Store.unquote(opts[:type] || :buffered)(unquote(opts))
    end
  end

  defmacro buffered(opts \\ []) do
    timeout = opts[:timeout] || 60_000
    quote do
      use GenServer

      def start_link(init \\ [], opts \\ []) do
        GenServer.start_link(__MODULE__, init, opts)
      end

      def store(entry) do
        :ets.insert(__MODULE__, {current_key(), entry})
      end

      def current_key do
        __MODULE__.Pointer.key()
      end

      defp update_key(ts) do
        quote do
          defmodule unquote(__MODULE__.Pointer) do
            def key, do: unquote(ts)
          end
        end
        |> Snow.Utils.eval_quoted()
      end

      # Callbacks

      def init(_) do
        :ets.new(__MODULE__, [:named_table,
                              {:write_concurrency, true},
                              :public,
                              :duplicate_bag])
        ts = tick()
        {:ok, ts}
      end

      if function_exported?(:erlang, :timestamp, 0) do
        defp timestamp do
          {mega, sec, micro} = :erlang.timestamp()
          mega * 1_000_000_000_000 + sec * 1_000_000 + micro
        end
      else
        defp timestamp do
          {mega, sec, micro} = :erlang.now()
          mega * 1_000_000_000_000 + sec * 1_000_000 + micro
        end
      end

      defp tick do
        ts = timestamp()
        update_key(ts)
        Process.send_after(self(), :flush, unquote(timeout))
        ts
      end

      def handle_call(_, _from, state) do
        {:noreply, state}
      end

      def handle_cast(_, state) do
        {:noreply, state}
      end

      def handle_info(:flush, ts) do
        new_ts = tick()
        case fetch(ts) do
          nil ->
            nil
          objs ->
            try do
              flush(ts, objs)
            catch
              type, error ->
                Exception.format(type, error)
                |> Logger.error()
                :ets.insert(__MODULE__, for obj <- objs do
                  {new_ts, obj}
                end)
                :ok
            end
        end
        :ets.delete(__MODULE__, ts)
        {:noreply, new_ts}
      end

      defp fetch(ts) do
        :ets.lookup_element(__MODULE__, ts, 2)
      catch
        _, _ ->
          nil
      end
    end
  end
end
