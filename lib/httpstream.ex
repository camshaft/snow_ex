defmodule HTTPStream do
  def get!(url, headers \\ [], options \\ []) do
    Stream.resource(
      fn ->
        opts = [{:stream_to, self()}, {:hackney, [follow_redirect: true]} | options]
        {HTTPoison.get!(url, headers, opts), headers, opts}
      end,
      &handle_response/1,
      fn (_) ->
        :ok
      end
    )
  end

  defp handle_response({%{id: ref} = resp, headers, opts} = state) do
    receive do
      %HTTPoison.AsyncStatus{id: ^ref, code: 200} ->
        handle_response(state)
      %HTTPoison.AsyncRedirect{id: ^ref, to: to, headers: _headers} ->
        resp = HTTPoison.get!(to, headers, opts)
        handle_response({resp, headers, opts})
      %HTTPoison.AsyncStatus{id: ^ref, code: code} ->
        raise(HTTPStream.Error, code: code)
      %HTTPoison.AsyncHeaders{id: ^ref} ->
        handle_response(state)
      %HTTPoison.AsyncChunk{id: ^ref, chunk: chunk} ->
        {[chunk], state}
      %HTTPoison.AsyncEnd{id: ^ref} ->
        {:halt, state}
    after
      10_000 ->
        raise HTTPStream.Timeout, ref: ref
    end
  end

  defmodule Collectable do
    defstruct method: nil,
              url: nil,
              headers: nil,
              options: nil
  end

  def post!(url, headers \\ [], options \\ []) do
    %__MODULE__.Collectable{method: "POST", url: url, headers: headers, options: options}
  end

  def put!(url, headers \\ [], options \\ []) do
    %__MODULE__.Collectable{method: "PUT", url: url, headers: headers, options: options}
  end

  defmodule Error do
    defexception [:code]

    def message(%{code: code}) do
      "Recieved status #{code}"
    end
  end

  defmodule Timeout do
    defexception [:ref]

    def message(%{ref: ref}) do
      "#{inspect(ref)} timed out after 10 seconds of inactivity"
    end
  end
end


defimpl Collectable, for: HTTPStream.Collectable do
  def into(%{method: method, url: url, headers: headers, options: options}) do
    {:ok, ref} = :hackney.request(method, url, headers, :stream, options)
    {ref, &handle/2}
  end

  defp handle(ref, {:cont, chunk}) do
    case :hackney.send_body(ref, chunk) do
      {:error, :closed} ->
        ref
        |> :hackney.start_response()
        |> IO.inspect
      :ok ->
        ref
    end
  end
  defp handle(ref, :done) do
    :ok = :hackney.finish_send_body(ref)
    :hackney.start_response(ref)
  end
  defp handle(ref, :halt) do
    :hackney.cancel_request(ref)
  end
end
