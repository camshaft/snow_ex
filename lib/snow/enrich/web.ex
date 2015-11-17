defmodule Snow.Enrich.Web do
  def contexts do
    __MODULE__.Document.contexts()
    ++ __MODULE__.Referer.contexts()
    ++ __MODULE__.Marketing.contexts()
    ++ __MODULE__.IPAddress.contexts()
    ++ __MODULE__.UserAgent.contexts()
    ++ __MODULE__.Browser.contexts()
  end

  def exec(stream) do
    stream
    |> __MODULE__.Document.exec()
    |> __MODULE__.Referer.exec()
    |> __MODULE__.Marketing.exec()
    |> __MODULE__.IPAddress.exec()
    |> __MODULE__.UserAgent.exec()
    |> __MODULE__.Browser.exec()
  end
end
