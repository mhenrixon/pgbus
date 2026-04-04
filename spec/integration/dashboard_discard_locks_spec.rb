# frozen_string_literal: true

require_relative "../integration_helper"

RSpec.describe "Dashboard discard operations clear locks (integration)", :integration do
  let(:client) { Pgbus.client }
  let(:data_source) { Pgbus::Web::DataSource.new(client: client) }
  let(:queue_name) { "default" }
  let(:full_queue) { Pgbus.configuration.queue_name(queue_name) }

  before do
    Pgbus::UniquenessKey.delete_all
    client.ensure_queue(queue_name)
  end

  describe "#discard_job clears associated lock" do
    it "releases the uniqueness lock when archiving a single enqueued message" do
      # Enqueue a message with uniqueness metadata
      payload = {
        "job_class" => "ImportJob",
        "arguments" => [42],
        Pgbus::Uniqueness::METADATA_KEY => "import-42",
        Pgbus::Uniqueness::STRATEGY_KEY => "until_executed",
        Pgbus::Uniqueness::TTL_KEY => 86_400
      }
      client.send_message(queue_name, payload)

      # Simulate the lock that was acquired at enqueue time
      Pgbus::UniquenessKey.acquire!("import-42", queue_name: queue_name, msg_id: 0)
      expect(Pgbus::UniquenessKey.locked?("import-42")).to be true

      # Read message to get msg_id
      msg = client.read_batch(queue_name, qty: 1, vt: 0).first

      # Discard via dashboard
      data_source.discard_job(full_queue, msg.msg_id)

      # Lock should be released
      expect(Pgbus::UniquenessKey.locked?("import-42")).to be false
    end

    it "does nothing for messages without uniqueness metadata" do
      payload = { "job_class" => "PlainJob", "arguments" => [] }
      client.send_message(queue_name, payload)

      msg = client.read_batch(queue_name, qty: 1, vt: 0).first

      expect { data_source.discard_job(full_queue, msg.msg_id) }.not_to raise_error
    end
  end

  describe "#discard_all_enqueued clears all associated locks" do
    it "archives all messages from all queues and releases their locks" do
      # Enqueue messages with uniqueness locks
      3.times do |i|
        payload = {
          "job_class" => "ImportJob",
          "arguments" => [i],
          Pgbus::Uniqueness::METADATA_KEY => "import-#{i}",
          Pgbus::Uniqueness::STRATEGY_KEY => "until_executed",
          Pgbus::Uniqueness::TTL_KEY => 86_400
        }
        client.send_message(queue_name, payload)
        Pgbus::UniquenessKey.acquire!("import-#{i}", queue_name: queue_name, msg_id: 0)
      end

      expect(Pgbus::UniquenessKey.count).to eq(3)

      count = data_source.discard_all_enqueued
      expect(count).to eq(3)

      # All locks should be released
      expect(Pgbus::UniquenessKey.count).to eq(0)

      # Queue should be empty
      messages = client.read_batch(queue_name, qty: 10, vt: 0)
      expect(messages).to be_empty
    end

    it "returns 0 when no messages exist" do
      count = data_source.discard_all_enqueued
      expect(count).to eq(0)
    end

    it "only clears locks for discarded messages, not unrelated locks" do
      # Enqueue one message with a lock
      payload = {
        "job_class" => "ImportJob",
        "arguments" => [1],
        Pgbus::Uniqueness::METADATA_KEY => "import-1",
        Pgbus::Uniqueness::STRATEGY_KEY => "until_executed",
        Pgbus::Uniqueness::TTL_KEY => 86_400
      }
      client.send_message(queue_name, payload)
      Pgbus::UniquenessKey.acquire!("import-1", queue_name: queue_name, msg_id: 0)

      # Create an unrelated lock (e.g., from a different system)
      Pgbus::UniquenessKey.acquire!("unrelated-lock", queue_name: "other", msg_id: 0)

      data_source.discard_all_enqueued

      # The enqueued message's lock should be cleared
      expect(Pgbus::UniquenessKey.locked?("import-1")).to be false
      # The unrelated lock should remain
      expect(Pgbus::UniquenessKey.locked?("unrelated-lock")).to be true
    end
  end

  describe "#discard_failed_event clears associated lock" do
    before do
      ActiveRecord::Base.connection.execute(<<~SQL)
        INSERT INTO pgbus_failed_events (queue_name, payload, headers, error_class, error_message, failed_at)
        VALUES ('#{full_queue}',
                '{"job_class":"FailedJob","arguments":[1],"pgbus_uniqueness_key":"failed-1","pgbus_uniqueness_strategy":"until_executed"}',
                '{}', 'RuntimeError', 'boom', NOW())
      SQL
    end

    it "releases the lock when discarding a failed event" do
      Pgbus::UniquenessKey.acquire!("failed-1", queue_name: queue_name, msg_id: 0)
      expect(Pgbus::UniquenessKey.locked?("failed-1")).to be true

      event = data_source.failed_events.first
      data_source.discard_failed_event(event["id"])

      expect(Pgbus::UniquenessKey.locked?("failed-1")).to be false
    end
  end

  describe "#discard_all_failed clears associated locks" do
    before do
      3.times do |i|
        ActiveRecord::Base.connection.execute(<<~SQL)
          INSERT INTO pgbus_failed_events (queue_name, payload, headers, error_class, error_message, failed_at)
          VALUES ('#{full_queue}',
                  '{"job_class":"FailedJob","arguments":[#{i}],"pgbus_uniqueness_key":"failed-#{i}","pgbus_uniqueness_strategy":"until_executed"}',
                  '{}', 'RuntimeError', 'boom #{i}', NOW())
        SQL
        Pgbus::UniquenessKey.acquire!("failed-#{i}", queue_name: queue_name, msg_id: 0)
      end
    end

    it "releases all locks for discarded failed events" do
      expect(Pgbus::UniquenessKey.count).to eq(3)

      data_source.discard_all_failed

      expect(Pgbus::UniquenessKey.count).to eq(0)
    end
  end

  describe "#discard_dlq_message clears associated lock" do
    let(:dlq_name) { Pgbus.configuration.dead_letter_queue_name(queue_name) }
    let(:full_dlq) { dlq_name }

    before do
      client.ensure_dead_letter_queue(queue_name)
    end

    it "releases the lock when discarding a DLQ message" do
      # Send a message directly to the DLQ
      payload = {
        "job_class" => "DlqJob",
        "arguments" => [1],
        Pgbus::Uniqueness::METADATA_KEY => "dlq-1",
        Pgbus::Uniqueness::STRATEGY_KEY => "until_executed",
        Pgbus::Uniqueness::TTL_KEY => 86_400
      }
      ActiveRecord::Base.connection.execute(
        "INSERT INTO pgmq.q_#{full_dlq} (vt, message) VALUES (NOW(), '#{JSON.generate(payload)}')"
      )

      Pgbus::UniquenessKey.acquire!("dlq-1", queue_name: queue_name, msg_id: 0)
      expect(Pgbus::UniquenessKey.locked?("dlq-1")).to be true

      msg = ActiveRecord::Base.connection.select_one("SELECT msg_id FROM pgmq.q_#{full_dlq} LIMIT 1")
      data_source.discard_dlq_message(full_dlq, msg["msg_id"])

      expect(Pgbus::UniquenessKey.locked?("dlq-1")).to be false
    end
  end

  describe "#discard_all_dlq clears associated locks" do
    let(:dlq_name) { Pgbus.configuration.dead_letter_queue_name(queue_name) }
    let(:full_dlq) { dlq_name }

    before do
      client.ensure_dead_letter_queue(queue_name)
    end

    it "releases all locks for discarded DLQ messages" do
      3.times do |i|
        payload = {
          "job_class" => "DlqJob",
          "arguments" => [i],
          Pgbus::Uniqueness::METADATA_KEY => "dlq-#{i}",
          Pgbus::Uniqueness::STRATEGY_KEY => "until_executed",
          Pgbus::Uniqueness::TTL_KEY => 86_400
        }
        ActiveRecord::Base.connection.execute(
          "INSERT INTO pgmq.q_#{full_dlq} (vt, message) VALUES (NOW(), '#{JSON.generate(payload)}')"
        )
        Pgbus::UniquenessKey.acquire!("dlq-#{i}", queue_name: queue_name, msg_id: 0)
      end

      expect(Pgbus::UniquenessKey.count).to eq(3)

      data_source.discard_all_dlq

      expect(Pgbus::UniquenessKey.count).to eq(0)
    end
  end
end
