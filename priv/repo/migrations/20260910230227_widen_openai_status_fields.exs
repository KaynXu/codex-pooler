defmodule CodexPooler.Repo.Migrations.WidenOpenAIStatusFields do
  use Ecto.Migration

  def up do
    execute("SET LOCAL lock_timeout = '10s'")

    alter table(:openai_status_feed_states) do
      modify :etag, :string, size: 512
      modify :last_modified, :string, size: 128
      modify :last_error_code, :string, size: 80
      modify :content_hash, :string, size: 128
    end

    alter table(:openai_status_incidents) do
      modify :guid, :string, size: 512
      modify :title, :string, size: 4_000
      modify :status, :string, size: 32
      modify :summary, :string, size: 4_000
      modify :component, :string, size: 512
      modify :link, :string, size: 2_048
      modify :content_hash, :string, size: 128
    end
  end

  def down do
    execute("SET LOCAL lock_timeout = '10s'")

    # PostgreSQL rejects over-width values; never cast with a truncating USING expression.
    alter table(:openai_status_incidents) do
      modify :guid, :string, size: 255
      modify :title, :string, size: 255
      modify :status, :string, size: 255
      modify :summary, :string, size: 255
      modify :component, :string, size: 255
      modify :link, :string, size: 255
      modify :content_hash, :string, size: 255
    end

    alter table(:openai_status_feed_states) do
      modify :etag, :string, size: 255
      modify :last_modified, :string, size: 255
      modify :last_error_code, :string, size: 255
      modify :content_hash, :string, size: 255
    end
  end
end
