defmodule Quarry.SelectTest do
  use ExUnit.Case
  doctest Quarry.Select

  import Ecto.Query
  alias Quarry.{Post, Select}

  setup do
    %{base: {from(p in Post, as: :post), []}}
  end

  test "can select single top level field", %{base: base} do
    expected =
      from(
        p in Post,
        as: :post,
        select: %{title: as(:post).title}
      )

    select = :title
    assert {actual, []} = Select.build(base, select)
    assert inspect(actual) == inspect(expected)
  end

  test "can select single top level field as list", %{base: base} do
    expected =
      from(
        p in Post,
        as: :post,
        select: %{title: as(:post).title}
      )

    select = [:title]
    assert {actual, []} = Select.build(base, select)
    assert inspect(actual) == inspect(expected)
  end

  test "can select multiple top level fields", %{base: base} do
    expected =
      from(
        p in Post,
        as: :post,
        select: %{id: as(:post).id, title: as(:post).title}
      )

    select = [:id, :title]
    assert {actual, []} = Select.build(base, select)
    assert inspect(actual) == inspect(expected)
  end

  test "can select nested field through association", %{base: base} do
    expected =
      from(
        p in Post,
        as: :post,
        left_join: a in assoc(p, :author),
        as: :post_author,
        select: %{publisher: as(:post_author).publisher}
      )

    select = [[:author, :publisher]]
    assert {actual, []} = Select.build(base, select)
    assert inspect(actual) == inspect(expected)
  end

  test "can select multiple nested fields", %{base: base} do
    expected =
      from(
        p in Post,
        as: :post,
        left_join: a in assoc(p, :author),
        as: :post_author,
        left_join: u in assoc(a, :user),
        as: :post_author_user,
        select: %{name: as(:post_author_user).name, publisher: as(:post_author).publisher}
      )

    select = [[:author, :user, :name], [:author, :publisher]]
    assert {actual, []} = Select.build(base, select)
    assert inspect(actual) == inspect(expected)
  end

  test "can select mix of top level and nested fields", %{base: base} do
    expected =
      from(
        p in Post,
        as: :post,
        left_join: a in assoc(p, :author),
        as: :post_author,
        select: %{title: as(:post).title, publisher: as(:post_author).publisher}
      )

    select = [:title, [:author, :publisher]]
    assert {actual, []} = Select.build(base, select)
    assert inspect(actual) == inspect(expected)
  end

  test "ignores bad top level field and returns error", %{base: base} do
    select = :bad_field
    {actual, [error]} = Select.build(base, select)

    assert inspect(actual) == inspect(elem(base, 0))
    assert %{type: :select, path: [:bad_field]} = error
  end

  test "ignores bad nested field and returns error", %{base: base} do
    select = [[:author, :bad_field]]
    {actual, [error]} = Select.build(base, select)

    assert inspect(actual) == inspect(elem(base, 0))
    assert %{type: :select, path: [:bad_field]} = error
  end

  test "ignores bad association and returns error", %{base: base} do
    select = [[:bad_association, :field]]
    {actual, [error]} = Select.build(base, select)

    assert inspect(actual) == inspect(elem(base, 0))
    assert %{type: :select, path: [:bad_association]} = error
  end

  test "returns passed in load_path on errors", %{base: base} do
    {_, [error]} = Select.build(base, [[:bad_association, :field]], [:post, :comments])
    assert %{type: :select, path: [:bad_association], load_path: [:comments, :post]} = error
  end

  test "can select from has_many association", %{base: base} do
    expected =
      from(
        p in Post,
        as: :post,
        left_join: c in assoc(p, :comments),
        as: :post_comments,
        select: %{body: as(:post_comments).body}
      )

    select = [[:comments, :body]]
    assert {actual, []} = Select.build(base, select)
    assert inspect(actual) == inspect(expected)
  end

  test "can select deeply nested field", %{base: base} do
    expected =
      from(
        p in Post,
        as: :post,
        left_join: a in assoc(p, :author),
        as: :post_author,
        left_join: u in assoc(a, :user),
        as: :post_author_user,
        select: %{name: as(:post_author_user).name}
      )

    select = [[:author, :user, :name]]
    assert {actual, []} = Select.build(base, select)
    assert inspect(actual) == inspect(expected)
  end

  test "handles empty select list", %{base: base} do
    select = []
    assert {actual, []} = Select.build(base, select)
    assert inspect(actual) == inspect(elem(base, 0))
  end
end
