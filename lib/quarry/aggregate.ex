defmodule Quarry.Aggregate do
  @moduledoc false
  require Ecto.Query

  alias Quarry.{Join, From}

  @type aggregate :: %{optional(atom()) => atom() | [atom()]}

  @spec build({Ecto.Query.t(), [Quarry.error()]}, [Quarry.aggregate_spec()], Quarry.group_by(), [atom()]) ::
          {Ecto.Query.t(), [Quarry.error()]}
  def build({query, errors}, aggregates, group_by, load_path \\ []) do
    root_binding = From.get_root_binding(query)
    schema = From.get_root_schema(query)

    # Process group_by first to set up joins
    {query, errors} = process_group_by({query, errors}, group_by, load_path)

    # Process aggregates one at a time
    aggregate({query, errors}, aggregates,
      binding: root_binding,
      schema: schema,
      path: [],
      load_path: load_path
    )
  end

  defp process_group_by({query, errors}, group_by, load_path) when is_list(group_by) do
    if Enum.empty?(group_by) do
      {query, errors}
    else
      root_binding = From.get_root_binding(query)
      schema = From.get_root_schema(query)

      state = [
        schema: schema,
        binding: root_binding,
        load_path: load_path
      ]

      # Check if this is a single nested path (list of atoms) or multiple fields
      case group_by do
        [field] when is_atom(field) ->
          # Single field
          process_group_by_field({query, errors}, field, [], state)
        fields when is_list(fields) and length(fields) > 1 ->
          # Check if all elements are atoms (nested path) or mixed (multiple fields)
          if Enum.all?(fields, &is_atom/1) do
            # This is a single nested path like [:author, :publisher]
            process_group_by_field({query, errors}, fields, [], state)
          else
            # Multiple fields - each could be an atom or a nested path
            process_group_by_fields({query, errors}, fields, [], state)
          end
        [nested_path] when is_list(nested_path) ->
          # Single nested path like [:author, :publisher]
          process_group_by_field({query, errors}, nested_path, [], state)
      end
    end
  end

  defp process_group_by({query, errors}, group_by, load_path) when not is_nil(group_by) do
    process_group_by({query, errors}, [group_by], load_path)
  end

  defp process_group_by(acc, nil, _load_path), do: acc

  defp process_group_by_fields({query, errors}, group_by_fields, join_deps, state) do
    Enum.reduce(group_by_fields, {query, errors}, fn field, acc ->
      process_group_by_field(acc, field, join_deps, state)
    end)
  end

  defp process_group_by_field({query, errors}, [field_name], join_deps, state) do
    process_group_by_field({query, errors}, field_name, join_deps, state)
  end

  defp process_group_by_field({query, errors}, [assoc | path], join_deps, state) do
    schema = state[:schema]
    associations = schema.__schema__(:associations)

    if assoc in associations do
      child_schema = schema.__schema__(:association, assoc).related
      state = Keyword.put(state, :schema, child_schema)
      process_group_by_field({query, errors}, path, [assoc | join_deps], state)
    else
      error = build_group_by_error(assoc, join_deps, state)
      {query, [error | errors]}
    end
  end

  defp process_group_by_field({query, errors}, field_name, join_deps, state) when is_atom(field_name) do
    if field_name in state[:schema].__schema__(:fields) do
      {query, join_binding} = Join.join_dependencies(query, state[:binding], Enum.reverse(join_deps))
      query = Ecto.Query.group_by(query, [field(as(^join_binding), ^field_name)])
      {query, errors}
    else
      error = build_group_by_error(field_name, join_deps, state)
      {query, [error | errors]}
    end
  end

  defp build_group_by_error(field, path, state) do
    %{
      type: :group_by,
      path: Enum.reverse([field | path]),
      load_path: Enum.reverse(state[:load_path]),
      message: "Quarry couldn't find field \"#{field}\" on Ecto schema \"#{state[:schema]}\""
    }
  end

  defp aggregate(acc, aggregates, state) do
    {query, errors, select_map} = Enum.reduce(aggregates, {elem(acc, 0), elem(acc, 1), %{}}, fn entry, {q, e, select_map} ->
      maybe_aggregate_field(entry, {q, e, select_map}, state)
    end)

    if map_size(select_map) > 0 do
      query = Ecto.Query.select(query, ^select_map)
      {query, errors}
    else
      {query, errors}
    end
  end

  defp maybe_aggregate_field({func, field} = entry, {query, errors, select_map}, state) do
    fields = state[:schema].__schema__(:fields)
    associations = state[:schema].__schema__(:associations)

    if (is_list(field) && hd(field) in associations) || field in fields do
      aggregate_field(entry, {query, errors, select_map}, state)
    else
      {query, [build_error(func, field, state) | errors], select_map}
    end
  end

  defp build_error(_func, field, state) do
    field_name = if is_list(field), do: hd(field), else: field
    %{
      type: :aggregate,
      path: Enum.reverse([field_name | state[:path]]),
      load_path: Enum.reverse(state[:load_path]),
      message: "Quarry couldn't find field \"#{field_name}\" on Ecto schema \"#{state[:schema]}\""
    }
  end

  defp aggregate_field({func, [assoc | path]}, {query, errors, select_map}, state) when is_list(path) do
    child_schema = state[:schema].__schema__(:association, assoc).related

    state =
      state
      |> Keyword.put(:schema, child_schema)
      |> Keyword.update!(:path, &List.insert_at(&1, 0, assoc))

    # If path is now a single element, convert it to an atom
    case path do
      [field] when is_atom(field) -> aggregate_field({func, field}, {query, errors, select_map}, state)
      _ -> aggregate_field({func, path}, {query, errors, select_map}, state)
    end
  end

  defp aggregate_field({func, field}, {query, errors, select_map}, state) when is_atom(field) do
    {query, join_binding} = Join.join_dependencies(query, state[:binding], state[:path])

    select_expr = aggregate_by_function(func, field, join_binding)
    select_map = Map.put(select_map, func, select_expr)
    {query, errors, select_map}
  end

  defp aggregate_by_function(:count, field, join_binding) do
    Ecto.Query.dynamic([], count(field(as(^join_binding), ^field)))
  end

  defp aggregate_by_function(:sum, field, join_binding) do
    Ecto.Query.dynamic([], sum(field(as(^join_binding), ^field)))
  end

  defp aggregate_by_function(:avg, field, join_binding) do
    Ecto.Query.dynamic([], avg(field(as(^join_binding), ^field)))
  end

  defp aggregate_by_function(:min, field, join_binding) do
    Ecto.Query.dynamic([], min(field(as(^join_binding), ^field)))
  end

  defp aggregate_by_function(:max, field, join_binding) do
    Ecto.Query.dynamic([], max(field(as(^join_binding), ^field)))
  end

end
