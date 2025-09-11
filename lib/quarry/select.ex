defmodule Quarry.Select do
  @moduledoc false
  require Ecto.Query

  alias Quarry.{Join, From}

  @type select :: atom() | [atom() | [atom()]]

  @spec build({Ecto.Query.t(), [Quarry.error()]}, select(), [atom()]) ::
          {Ecto.Query.t(), [Quarry.error()]}
  def build({query, errors}, select_params, load_path \\ []) do
    root_binding = From.get_root_binding(query)
    schema = From.get_root_schema(query)

    select({query, errors}, select_params,
      binding: root_binding,
      schema: schema,
      path: [],
      load_path: load_path
    )
  end

  defp select(acc, select_params, state) do
    select_params
    |> List.wrap()
    |> Enum.reduce(acc, &maybe_select_field(&2, &1, state))
  end

  defp maybe_select_field({query, errors}, field_name, state) when is_atom(field_name) do
    fields = state[:schema].__schema__(:fields)

    if field_name in fields do
      select_field({query, errors}, field_name, state)
    else
      {query, [build_error(field_name, state) | errors]}
    end
  end

  defp maybe_select_field({query, errors}, [field_name], state) when is_atom(field_name) do
    # Handle single field in list format like [:author_id]
    maybe_select_field({query, errors}, field_name, state)
  end

  defp maybe_select_field({query, errors}, [association, field_name], state) when is_atom(association) and is_atom(field_name) do
    # Handle nested field like [:author, :id]
    associations = state[:schema].__schema__(:associations)

    if association in associations do
      select_nested_field({query, errors}, association, field_name, state)
    else
      {query, [build_error(association, state) | errors]}
    end
  end

  defp maybe_select_field({query, errors}, [association | rest], state) when is_atom(association) and is_list(rest) do
    # Handle deeply nested field like [:author, :user, :name]
    associations = state[:schema].__schema__(:associations)

    if association in associations do
      child_schema = state[:schema].__schema__(:association, association).related
      child_state = Keyword.put(state, :schema, child_schema)
      child_state = Keyword.update!(child_state, :path, &[association | &1])

      maybe_select_field({query, errors}, rest, child_state)
    else
      {query, [build_error(association, state) | errors]}
    end
  end

  defp maybe_select_field({query, errors}, field_name, state) do
    {query, [build_error(field_name, state) | errors]}
  end

  defp build_error(field_name, state) do
    field_name_str = if is_list(field_name), do: inspect(field_name), else: to_string(field_name)

    %{
      type: :select,
      path: Enum.reverse([field_name | state[:path]]),
      load_path: Enum.reverse(state[:load_path]),
      message: "Quarry couldn't find field \"#{field_name_str}\" on Ecto schema \"#{state[:schema]}\""
    }
  end

  defp select_field({query, errors}, field_name, state) do
    {query, join_binding} = Join.join_dependencies(query, state[:binding], state[:path])

    # Check if query already has a select clause
    query = if has_select?(query) do
      Ecto.Query.select_merge(query, %{^field_name => field(as(^join_binding), ^field_name)})
    else
      Ecto.Query.select(query, %{^field_name => field(as(^join_binding), ^field_name)})
    end

    {query, errors}
  end

  defp select_nested_field({query, errors}, association, field_name, state) do
    child_schema = state[:schema].__schema__(:association, association).related
    child_fields = child_schema.__schema__(:fields)

    if field_name in child_fields do
      {query, join_binding} = Join.join_dependencies(query, state[:binding], [association | state[:path]])

      # Check if query already has a select clause
      query = if has_select?(query) do
        Ecto.Query.select_merge(query, %{^field_name => field(as(^join_binding), ^field_name)})
      else
        Ecto.Query.select(query, %{^field_name => field(as(^join_binding), ^field_name)})
      end

      {query, errors}
    else
      {query, [build_error(field_name, state) | errors]}
    end
  end

  defp has_select?(%Ecto.Query{select: nil}), do: false
  defp has_select?(%Ecto.Query{}), do: true
end
