defmodule Snow.Utils do
  def eval_quoted(quoted, context \\ []) do
    opts = Code.compiler_options
    Code.compiler_options(put_in(opts, [:ignore_module_conflict], true))
    out = Code.eval_quoted(quoted, context)
    Code.compiler_options(opts)
    out
  end

  def get_json!(url) do
    %{body: body, status_code: 200} = HTTPoison.get!(url)
    body
    |> Poison.decode!()
  end

  def timestamp do
    {mega, sec, microsec} = :os.timestamp()
    mega * 1_000_000_000 + sec * 1_000 + div(microsec, 1_000)
  end

  def format_ip({a,b,c,d}) do
    "#{a}.#{b}.#{c}.#{d}"
  end
  def format_ip({a,b,c,d,e,f,g,h}) do
    to_hex(a) <> ":" <>
      to_hex(b) <> ":" <>
      to_hex(c) <> ":" <>
      to_hex(d) <> ":" <>
      to_hex(e) <> ":" <>
      to_hex(f) <> ":" <>
      to_hex(g) <> ":" <>
      to_hex(h)
  end
  def format_ip(nil) do
    nil
  end

  defp to_hex(num) do
    :erlang.integer_to_binary(num, 16)
  end
end
