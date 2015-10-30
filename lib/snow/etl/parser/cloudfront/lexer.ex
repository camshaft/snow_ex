defmodule Snow.ETL.Parser.Cloudfront.Lexer do
  def consume_tab(chunk) do
    consume(chunk, "\t")
  end

  def consume_nl(chunk) do
    consume(chunk, "\n")
  end

  for i <- 1..1_000 do
    def consume(<<value :: size(unquote(i))-binary, char :: size(1)-binary, rest :: binary>>, char) do
      {value, rest}
    end
  end
  def consume(_, _) do
    throw :cont
  end
end
