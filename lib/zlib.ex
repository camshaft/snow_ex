defmodule Zlib do
  @max_wbits 15

  defmodule Deflate do
    defstruct level: :default,
              method: :deflated,
              window: 15,
              mem_level: 8,
              strategy: :default
  end

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

  def gzip_collectable() do
    %Zlib.Deflate{window: 16 + @max_wbits}
  end

  def gunzip_stream(stream, size \\ @max_wbits + 16) do
    Stream.resource(
      fn ->
        z = :zlib.open()
        :ok = :zlib.inflateInit(z, size)
        {stream, z, nil, []}
      end,
      &handle_inflate/1,
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

  defp handle_deflate({stream, z, :finished}) do
    {:halt, {stream, z, :finished}}
  end
  defp handle_deflate({stream, z, mode}) do
    case Nile.Utils.next(stream) do
      {status, _} when status in [:done, :halted] ->
        out = :zlib.deflate(z, [], :finish)
        {out, {stream, z, :finished}}
      {:suspended, chunk, stream} ->
        out = :zlib.deflate(z, chunk)
        {out, {stream, z, mode}}
    end
  end

  defp handle_inflate({stream, z, :finished, buffer}) do
    {:halt, {stream, z, :finished, buffer}}
  end
  defp handle_inflate({stream, z, mode, buffer}) do
    case Nile.Utils.next(stream) do
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

defimpl Collectable, for: Zlib.Deflate do
  def into(deflate) do
    z = :zlib.open()
    :ok = :zlib.deflateInit(z, deflate.level,
                               deflate.method,
                               deflate.window,
                               deflate.mem_level,
                               deflate.strategy)
    {{z, []}, &deflate/2}
  end

  defp deflate({z, acc}, {:cont, chunk}) do
    case :zlib.deflate(z, chunk) do
      [] ->
        {z, acc}
      out ->
        {z, [acc, out]}
    end
  end
  defp deflate({z, acc}, :done) do
    out = :zlib.deflate(z, [], :finish)
    :ok = :zlib.deflateEnd(z)
    :ok = :zlib.close(z)
    [acc, out]
  end
  defp deflate({z, _}, :halt) do
    :zlib.close(z)
    nil
  end
end
