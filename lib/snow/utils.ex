defmodule Snow.Utils do
  def eval_quoted(quoted, context \\ []) do
    opts = Code.compiler_options
    Code.compiler_options([{:ignore_module_conflict, true} | opts])
    out = Code.eval_quoted(quoted, context)
    Code.compiler_options(opts)
    out
  end
end
