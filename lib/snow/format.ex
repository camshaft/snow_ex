defmodule Snow.Format do
  def encode(_line) do

  end

  def decode_stream(stream) do
    Stream.transform(stream, [], fn(chunk, acc) ->
      unpack([acc, chunk], [])
    end)
  end

  defp unpack(buffer, acc) do
    case Msgpax.unpack_slice(buffer) do
      {:error, _} ->
        {:lists.reverse(acc), buffer}
      {:ok, value, rest} ->
        value = Snow.Payload.from_dict(value)
        unpack(rest, [value | acc])
    end
  end
end
