defmodule Zlib do
  @max_wbits 15

  def gzip_stream(stream) do
    Stream.resource(
      fn ->
        z = :zlib.open()
        :ok = :zlib.deflateInit(z, :default, :deflated, 16 + @max_wbits, 8, :default)
        {stream, z, nil}
      end,
      &handle_deflate/1,
      fn
        ({_, z, :finished}) ->
          :ok = :zlib.deflateEnd(z)
          :zlib.close(z)
        ({_, z, _}) ->
          try do
            :zlib.close(z)
          catch
            _, _ ->
              :ok
          end
      end
    )
  end

  def gunzip_stream(stream, size \\ @max_wbits + 16) do
    Stream.resource(
      fn ->
        z = :zlib.open()
        :ok = :zlib.inflateInit(z, size)
        {stream, z, nil, []}
      end,
      &handle_inflate1/1,
      fn
        ({_, z, :finished, _}) ->
          :zlib.close(z)
        ({_, z, _, _}) ->
          try do
            :zlib.close(z)
          catch
            _, _ ->
              :ok
          end
      end
    )
  end

  defp next(nil) do
    {:done, nil}
  end
  defp next({:cont, reducer}) when is_function(reducer) do
    {:cont, []}
    |> reducer.()
    |> wrap_cont()
  end
  defp next(reducer) when is_function(reducer) do
    {:cont, []}
    |> reducer.(fn(value, _) -> {:suspend, value} end)
    |> wrap_cont()
  end
  defp next(stream) do
    stream
    |> Enumerable.reduce({:cont, []}, fn(value, _) -> {:suspend, value} end)
    |> wrap_cont()
  end

  defp wrap_cont({:suspended, value, stream}) do
    {:suspended, value, {:cont, stream}}
  end
  defp wrap_cont(other) do
    other
  end

  defp handle_deflate({stream, z, :finished}) do
    {:halt, {stream, z, :finished}}
  end
  defp handle_deflate({stream, z, mode}) do
    case next(stream) do
      {status, _} when status in [:done, :halted] ->
        out = :zlib.deflate(z, [], :finish)
        {out, {stream, z, :finished}}
      {:suspended, chunk, stream} ->
        out = :zlib.deflate(z, chunk)
        {out, {stream, z, mode}}
    end
  end

  defp handle_inflate1(a) do
    handle_inflate(a)
  end

  defp handle_inflate({stream, z, :finished, buffer}) do
    {:halt, {stream, z, :finished, buffer}}
  end
  defp handle_inflate({stream, z, mode, buffer}) do
    case next(stream) do
      {status, _} when status in [:done, :halted] ->
        {o, buffer} = inflate_chunk(z, [], buffer)
        :zlib.inflateEnd(z)
        {o, {stream, z, :finished, buffer}}
      {:suspended, chunk, stream} ->
        {o, buffer} = inflate_chunk(z, chunk, buffer)
        {o, {stream, z, mode, buffer}}
    end
  end

  defp inflate_chunk(z, chunk, buffer) do
    {:zlib.inflate(z, [chunk, buffer]), []}
  end
end
