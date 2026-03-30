# Coding Style Rules

## File Organization

**MANY SMALL FILES > FEW LARGE FILES**

- High cohesion, low coupling
- 200-400 lines typical
- 800 lines maximum per file
- Extract complex logic to dedicated classes
- Organize by concern (client, adapter, event_bus, process, web)

## Ruby Style

### Classes & Methods

```ruby
# Good: Small, focused methods
def execute(message, queue_name)
  check_dead_letter(message, queue_name)
  deserialize_and_run(message)
  archive(message, queue_name)
end

# Bad: Giant methods doing everything
def process_everything
  # 200 lines of code...
end
```

### Error Handling

```ruby
# Good: Specific error handling
def execute(message, queue_name)
  job = deserialize(message)
  job.perform_now
  client.archive_message(queue_name, message.msg_id.to_i)
  :success
rescue ActiveJob::DeserializationError => e
  Pgbus.logger.error { "[Pgbus] Deserialization failed: #{e.message}" }
  :failed
rescue StandardError => e
  Pgbus.logger.error { "[Pgbus] Job failed: #{e.message}" }
  :failed
end

# Bad: Swallowing errors
def execute(message, queue_name)
  deserialize(message).perform_now
rescue StandardError
  nil
end
```

### PGMQ Operations

```ruby
# Good: All PGMQ access through Client
Pgbus.client.send_message(queue, payload)
Pgbus.client.read_batch(queue, qty: 5)

# Bad: Direct PGMQ calls
pgmq = PGMQ::Client.new(...)
pgmq.produce("raw_queue_name", data)
```

### Thread Safety

```ruby
# Good: Mutex for shared state
@mutex.synchronize { @queues_created[name] = true }

# Good: Concurrent primitives
@pool = Concurrent::FixedThreadPool.new(threads)

# Bad: Unsynchronized shared state
@queues_created[name] = true  # race condition
```

## Code Quality Checklist

Before marking work complete:
- [ ] Code is readable and well-named
- [ ] Methods are small (<30 lines ideal, <50 max)
- [ ] Files are focused (<800 lines)
- [ ] No deep nesting (>4 levels)
- [ ] Proper error handling with logging
- [ ] All PGMQ operations through Client
- [ ] Thread safety verified for shared state
- [ ] Rubocop passes
