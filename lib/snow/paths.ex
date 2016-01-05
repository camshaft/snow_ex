defmodule Snow.Paths do
  defmacro __using__(opts) do
    matches = (opts[:pattern] || "jsonpaths/*.json")
    |> Code.eval_quoted([], __CALLER__)
    |> elem(0)
    |> format()

    do_match_quoted = matches
    |> Enum.map(&to_quoted/1)

    names = matches
    |> Enum.map(&elem(&1, 0))

    quote do
      def list do
        unquote(names)
      end

      def match(name, %Snow.Model.Context{} = model) do
        match(name, Snow.Model.Context.to_map(model))
      end
      def match(name, data) do
        name
        |> String.downcase()
        |> do_match(data)
      end

      defp do_match("events", data) do
        data
        |> Snow.Model.Event.to_list()
        |> Enum.map(&Snow.Paths.escape/1)
      end
      unquote_splicing(do_match_quoted)
    end
  end

  def escape(nil) do
    nil
  end
  def escape(value) when is_binary(value) do
    [8, value, 8]
  end
  def escape([]) do
    nil
  end
  def escape(value) when is_list(value) do
    value
    |> Poison.encode!()
    |> escape()
  end
  def escape(%{__struct__: _} = s) do
    case String.Chars.impl_for(s) do
      nil ->
        s |> Poison.encode!() |> escape()
      impl ->
        s |> impl.to_string() |> escape()
    end
  end
  def escape(v) when is_map(v) do
    v
    |> Poison.encode!()
    |> escape()
  end
  def escape(other) do
    other
    |> to_string()
    |> escape()
  end

  defp format(pattern) do
    pattern
    |> Path.wildcard()
    |> Stream.map(&read_file/1)
    |> Stream.map(&decode_file/1)
    |> Stream.map(fn({name, %{"jsonpaths" => jsonpaths}}) ->
      {name, nest_paths(jsonpaths)}
    end)
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
        unquote(Enum.map(list, fn(i) ->
          quote do
            Snow.Paths.escape(unquote(ivar(i)))
          end
        end))
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
        [assign_key(parent, key, ivar(value)) | acc]
      ({key, value}, acc) when is_map(value) ->
        name = {parent, key} |> :erlang.phash2() |> ivar()
        extract_quoted(value, [assign_key(parent, key, name, Macro.escape(%{})) | acc], name)
    end)
  end

  defp assign_key(parent, key, var, default \\ nil) do
    quote do
      unquote(var) = Map.get(unquote(parent), unquote(key)) || Map.get(unquote(parent), unquote(String.to_atom(key))) || unquote(default)
    end
  end

  defp ivar(i) do
    Macro.var(:"_#{i}", nil)
  end
end
