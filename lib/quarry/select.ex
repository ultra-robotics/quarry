defmodule Quarry.Select do
  @moduledoc false
  require Ecto.Query
  require Quarry.Fragments

  alias Quarry.{Join, From}

  @type select :: atom() | [atom() | [atom()]] | select_map()
  @type select_map :: %{field: [atom()], as: atom(), fragment: String.t()}

  # Registry of pre-defined fragments
  @fragments %{
    "UPPER(?)" => :upper,
    "LOWER(?)" => :lower,
    "CONCAT(?, ' - ', ?)" => :concat,
    "date_trunc('day', ?)" => :date_trunc_day,
    "date_trunc('week', ?)" => :date_trunc_week,
    "date_trunc('month', ?)" => :date_trunc_month,
    "date_trunc('year', ?)" => :date_trunc_year
  }

  @spec build({Ecto.Query.t(), [Quarry.error()]}, select(), [atom()]) ::
          {Ecto.Query.t(), [Quarry.error()]}
  def build({query, errors}, select_params, load_path \\ []) do
    root_binding = From.get_root_binding(query)
    schema = From.get_root_schema(query)

    select_fields({query, errors}, select_params,
      binding: root_binding,
      schema: schema,
      path: [],
      load_path: load_path
    )
  end

  defp select_fields(acc, select_params, state) do
    select_params
    |> List.wrap()
    |> Enum.reduce({elem(acc, 0), elem(acc, 1), %{}}, &maybe_select_field(&2, &1, state))
    |> then(fn {query, errors, select_map} ->
      if map_size(select_map) > 0 do
        query = Ecto.Query.select(query, ^select_map)
        {query, errors}
      else
        {query, errors}
      end
    end)
  end

  defp maybe_select_field({query, errors, select_map}, field_name, state) when is_atom(field_name) do
    fields = state[:schema].__schema__(:fields)

    if field_name in fields do
      select_field({query, errors, select_map}, field_name, state)
    else
      {query, [build_error(field_name, state) | errors], select_map}
    end
  end

  defp maybe_select_field({query, errors, select_map}, [field_name], state) when is_atom(field_name) do
    # Handle single field in list format like [:author_id]
    maybe_select_field({query, errors, select_map}, field_name, state)
  end

  defp maybe_select_field({query, errors, select_map}, [association, field_name], state) when is_atom(association) and is_atom(field_name) do
    # Handle nested field like [:author, :id]
    associations = state[:schema].__schema__(:associations)

    if association in associations do
      select_nested_field({query, errors, select_map}, association, field_name, state)
    else
      {query, [build_error(association, state) | errors], select_map}
    end
  end

  defp maybe_select_field({query, errors, select_map}, [association | rest], state) when is_atom(association) and is_list(rest) do
    # Handle deeply nested field like [:author, :user, :name]
    associations = state[:schema].__schema__(:associations)

    if association in associations do
      child_schema = state[:schema].__schema__(:association, association).related
      child_state = Keyword.put(state, :schema, child_schema)
      child_state = Keyword.update!(child_state, :path, &[association | &1])

      maybe_select_field({query, errors, select_map}, rest, child_state)
    else
      {query, [build_error(association, state) | errors], select_map}
    end
  end

  defp maybe_select_field({query, errors, select_map}, %{field: field_path, as: as_name, fragment: fragment_sql}, state) do
    # Handle fragment with field path like %{field: [:author, :name], as: :author_name_upper, fragment: "UPPER(?)"}
    select_fragment({query, errors, select_map}, field_path, as_name, fragment_sql, state)
  end

  defp maybe_select_field({query, errors, select_map}, %{field: _field_path, fragment: _fragment_sql}, state) do
    # Handle fragment without required :as option
    {query, [build_fragment_error("Missing required :as option", state) | errors], select_map}
  end

  defp maybe_select_field({query, errors, select_map}, %{as: _as_name, fragment: _fragment_sql}, state) do
    # Handle fragment without required :field option
    {query, [build_fragment_error("Missing required :field option", state) | errors], select_map}
  end

  defp maybe_select_field({query, errors, select_map}, field_name, state) do
    {query, [build_error(field_name, state) | errors], select_map}
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

  defp build_fragment_error(message, state) do
    %{
      type: :select,
      path: Enum.reverse(state[:path]),
      load_path: Enum.reverse(state[:load_path]),
      message: message
    }
  end

  defp select_field({query, errors, select_map}, field_name, state) do
    {query, join_binding} = Join.join_dependencies(query, state[:binding], state[:path])

    # Add field to select_map instead of calling Ecto.Query.select directly
    select_expr = Ecto.Query.dynamic([], field(as(^join_binding), ^field_name))
    select_map = Map.put(select_map, field_name, select_expr)

    {query, errors, select_map}
  end

  defp select_nested_field({query, errors, select_map}, association, field_name, state) do
    child_schema = state[:schema].__schema__(:association, association).related
    child_fields = child_schema.__schema__(:fields)

    if field_name in child_fields do
      {query, join_binding} = Join.join_dependencies(query, state[:binding], [association | state[:path]])

      # Add field to select_map instead of calling Ecto.Query.select directly
      select_expr = Ecto.Query.dynamic([], field(as(^join_binding), ^field_name))
      select_map = Map.put(select_map, field_name, select_expr)

      {query, errors, select_map}
    else
      {query, [build_error(field_name, state) | errors], select_map}
    end
  end

  defp select_fragment({query, errors, select_map}, field_path, as_name, fragment_sql, state) do
    # Process the field path to get the final field and schema
    case process_field_path(field_path, state) do
      {:ok, final_field, final_schema, final_path} ->
        # Validate that the field exists
        fields = final_schema.__schema__(:fields)
        if final_field in fields do
          # Get the join binding for the field path
          {query, join_binding} = Join.join_dependencies(query, state[:binding], final_path)

          # Create the fragment expression using dynamic context
          fragment_expr = case @fragments[fragment_sql] do
            :upper ->
              Ecto.Query.dynamic([], fragment("UPPER(?)", field(as(^join_binding), ^final_field)))
            :lower ->
              Ecto.Query.dynamic([], fragment("LOWER(?)", field(as(^join_binding), ^final_field)))
            :concat ->
              Ecto.Query.dynamic([], fragment("CONCAT(?, ' - ', ?)", field(as(^join_binding), ^final_field), field(as(^join_binding), ^final_field)))
                 :date_trunc_day ->
                   Ecto.Query.dynamic([], fragment("date_trunc('day', ?)", field(as(^join_binding), ^final_field)))
                 :date_trunc_week ->
                   Ecto.Query.dynamic([], fragment("date_trunc('week', ?)", field(as(^join_binding), ^final_field)))
                 :date_trunc_month ->
                   Ecto.Query.dynamic([], fragment("date_trunc('month', ?)", field(as(^join_binding), ^final_field)))
                 :date_trunc_year ->
                   Ecto.Query.dynamic([], fragment("date_trunc('year', ?)", field(as(^join_binding), ^final_field)))
            nil ->
              # For custom fragments, we need to handle them differently
              # This is a limitation - we can't dynamically interpolate SQL strings
              raise ArgumentError, "Custom fragment SQL '#{fragment_sql}' is not supported. Use one of the pre-defined fragments: #{Map.keys(@fragments) |> Enum.join(", ")}"
          end

          # Add fragment to select_map instead of calling Ecto.Query.select directly
          select_map = Map.put(select_map, as_name, fragment_expr)

          {query, errors, select_map}
        else
          {query, [build_error(final_field, state) | errors], select_map}
        end
      {:error, error_field} ->
        {query, [build_error(error_field, state) | errors], select_map}
    end
  end


  defp process_field_path([field_name], state) when is_atom(field_name) do
    fields = state[:schema].__schema__(:fields)
    if field_name in fields do
      {:ok, field_name, state[:schema], state[:path]}
    else
      # Field doesn't exist on current schema, check if it exists through associations
      associations = state[:schema].__schema__(:associations)
      case find_field_through_associations(field_name, associations, state) do
        {:ok, schema, path} -> {:ok, field_name, schema, path}
        :error -> {:error, field_name}
      end
    end
  end

  defp process_field_path([association, field_name], state) when is_atom(association) and is_atom(field_name) do
    associations = state[:schema].__schema__(:associations)

    if association in associations do
      child_schema = state[:schema].__schema__(:association, association).related
      child_fields = child_schema.__schema__(:fields)

      if field_name in child_fields do
        # Field exists directly on the associated schema
        {:ok, field_name, child_schema, [association | state[:path]]}
      else
        # Field doesn't exist on the associated schema - this is an error
        # We don't automatically search through nested associations for two-level paths
        {:error, field_name}
      end
    else
      {:error, association}
    end
  end

  defp process_field_path([association | rest], state) when is_atom(association) and is_list(rest) do
    associations = state[:schema].__schema__(:associations)

    if association in associations do
      child_schema = state[:schema].__schema__(:association, association).related
      child_state = Keyword.put(state, :schema, child_schema)
      child_state = Keyword.update!(child_state, :path, &[association | &1])

      process_field_path(rest, child_state)
    else
      {:error, association}
    end
  end

  defp process_field_path(field_path, _state) do
    {:error, field_path}
  end

  defp find_field_through_associations(field_name, associations, state) do
    Enum.find_value(associations, :error, fn association ->
      child_schema = state[:schema].__schema__(:association, association).related
      child_fields = child_schema.__schema__(:fields)

      if field_name in child_fields do
        {:ok, child_schema, [association | state[:path]]}
      else
        # Check if the field exists through nested associations
        child_associations = child_schema.__schema__(:associations)
        child_state = Keyword.put(state, :schema, child_schema)
        child_state = Keyword.update!(child_state, :path, &[association | &1])

        case find_field_through_associations(field_name, child_associations, child_state) do
          {:ok, schema, path} -> {:ok, schema, path}
          :error -> nil
        end
      end
    end)
  end

end
