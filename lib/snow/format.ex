defmodule Snow.Format do
  def encode(line) do

  end

  def decode_stream(stream) do
    Stream.transform(stream, [], fn(chunk, acc) ->
      case Msgpax.unpack_slice([acc, chunk]) do
        {:ok, obj, acc} ->
          event = Snow.Model.from_obj(obj)
          {[event], acc}
      end
    end)
  end
end
