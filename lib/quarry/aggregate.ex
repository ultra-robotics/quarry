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

  defp process_group_by({query, errors}, group_by, _load_path) when is_list(group_by) do
    if Enum.empty?(group_by) do
      {query, errors}
    else
      # For now, just return the query without group_by
      # TODO: Implement proper group_by functionality
      {query, errors}
    end
  end

  defp process_group_by({query, errors}, group_by, load_path) when not is_nil(group_by) do
    process_group_by({query, errors}, [group_by], load_path)
  end

  defp process_group_by(acc, nil, _load_path), do: acc

  defp aggregate(acc, aggregates, state) do
    Enum.reduce(aggregates, acc, &maybe_aggregate_field(&1, &2, state))
  end

  defp maybe_aggregate_field({func, field} = entry, {query, errors}, state) do
    fields = state[:schema].__schema__(:fields)
    associations = state[:schema].__schema__(:associations)

    if (is_list(field) && hd(field) in associations) || field in fields do
      aggregate_field(entry, {query, errors}, state)
    else
      {query, [build_error(func, field, state) | errors]}
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

  defp aggregate_field({func, [assoc | path]}, acc, state) when is_list(path) do
    child_schema = state[:schema].__schema__(:association, assoc).related

    state =
      state
      |> Keyword.put(:schema, child_schema)
      |> Keyword.update!(:path, &List.insert_at(&1, 0, assoc))

    # If path is now a single element, convert it to an atom
    case path do
      [field] when is_atom(field) -> aggregate_field({func, field}, acc, state)
      _ -> aggregate_field({func, path}, acc, state)
    end
  end

  defp aggregate_field({func, field}, {query, errors}, state) when is_atom(field) do
    {query, join_binding} = Join.join_dependencies(query, state[:binding], state[:path])

    query = aggregate_by_function(query, func, field, join_binding)
    {query, errors}
  end

  defp aggregate_by_function(query, :count, field, join_binding) do
    select_expr = Ecto.Query.dynamic([{^join_binding, b}], count(field(b, ^field)))
    Ecto.Query.select_merge(query, ^%{count: select_expr})
  end

  defp aggregate_by_function(query, :sum, field, join_binding) do
    select_expr = Ecto.Query.dynamic([{^join_binding, b}], sum(field(b, ^field)))
    Ecto.Query.select_merge(query, ^%{sum: select_expr})
  end

  defp aggregate_by_function(query, :avg, field, join_binding) do
    select_expr = Ecto.Query.dynamic([{^join_binding, b}], avg(field(b, ^field)))
    Ecto.Query.select_merge(query, ^%{avg: select_expr})
  end

  defp aggregate_by_function(query, :min, field, join_binding) do
    select_expr = Ecto.Query.dynamic([{^join_binding, b}], min(field(b, ^field)))
    Ecto.Query.select_merge(query, ^%{min: select_expr})
  end

  defp aggregate_by_function(query, :max, field, join_binding) do
    select_expr = Ecto.Query.dynamic([{^join_binding, b}], max(field(b, ^field)))
    Ecto.Query.select_merge(query, ^%{max: select_expr})
  end

end
