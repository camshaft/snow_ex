defmodule Snow.Enrich.Common do
  def contexts do
    __MODULE__.Device.contexts()
    ++ __MODULE__.Session.contexts()
    ++ __MODULE__.User.contexts()
  end

  def exec(stream) do
    stream
    |> __MODULE__.Device.exec()
    |> __MODULE__.Session.exec()
    |> __MODULE__.User.exec()
  end
end
