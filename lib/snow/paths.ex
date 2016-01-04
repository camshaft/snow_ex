defmodule Snow.Paths do
  defmacro __using__(opts) do
    matches = format(opts[:pattern] || "jsonpaths/*.json")
    quote do
      def match(name, data) do
        do_match(name, data)
      end

      unquote_splicing(matches)
    end
  end

  defp format(pattern) do
    pattern
    |> Path.wildcard()
    |> Stream.map(&read_file/1)
    |> Stream.map(&decode_file/1)
    |> Stream.map(fn({name, %{"jsonpaths" => jsonpaths}}) ->
      {name, nest_paths(jsonpaths)}
    end)
    |> Enum.map(&to_quoted/1)
  end

  defp read_file(file) do
    {Path.basename(file, ".json"), File.read!(file)}
  end

  defp decode_file({name, json}) do
    {name, Poison.decode!(json)}
  end

  defp nest_paths(jsonpaths) do
    jsonpaths
    |> Enum.reduce({%{}, 0}, &nest_path/2)
    |> prepare_nested()
  end

  defp nest_path(path, {acc, count}) do
    acc = path
    |> String.split(".")
    |> tl()
    |> put_into(acc, count)

    {acc, count + 1}
  end

  defp put_into([path], acc, i) do
    Map.put(acc, path, i)
  end
  defp put_into([path | rest], acc, i) do
    value = Map.get(acc, path, %{})
    Map.put(acc, path, put_into(rest, value, i))
  end

  defp prepare_nested({paths, count}) do
    {0..(count - 1), paths}
  end

  defp to_quoted({name, {list, paths}}) do
    root = Macro.var(:root, nil)
    quote do
      defp do_match(unquote(name), unquote(root)) do
        unquote_splicing(extract_quoted(paths, root))
        unquote(list |> Enum.map(&ivar/1))
      end
    end
  end

  defp extract_quoted(paths, root) do
    paths
    |> extract_quoted([], root)
    |> Enum.reverse()
  end

  defp extract_quoted(data, acc, parent) do
    data
    |> Enum.reduce(acc, fn
      ({key, value}, acc) when is_integer(value) ->
        [quote do
          unquote(Macro.var(:"_#{value}", nil)) = Map.get(unquote(parent), unquote(key))
        end | acc]
      ({key, value}, acc) when is_map(value) ->
        name = {parent, key} |> :erlang.phash2() |> ivar()
        acc = [quote do
          unquote(name) = Map.get(unquote(parent), unquote(key), %{})
        end | acc]
        extract_quoted(value, acc, name)
    end)
  end

  defp ivar(i) do
    Macro.var(:"_#{i}", nil)
  end
end
