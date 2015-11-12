defmodule Snow.Utils do
  def eval_quoted(quoted, context \\ []) do
    opts = Code.compiler_options
    Code.compiler_options([{:ignore_module_conflict, true} | opts])
    out = Code.eval_quoted(quoted, context)
    Code.compiler_options(opts)
    out
  end

  def get_json!(url) do
    %{body: body, status_code: 200} = HTTPoison.get!(url)
    body
    |> Poison.decode!()
  end
end
