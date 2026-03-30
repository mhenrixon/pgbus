---
description: "Use when implementing any feature or fixing any bug -- enforces RED-GREEN-REFACTOR: write failing test first, implement minimum code to pass, then refactor."
---

# TDD Command

Enforce test-driven development methodology with RED -> GREEN -> REFACTOR cycle.

## The TDD Cycle

```text
RED -> GREEN -> REFACTOR -> REPEAT

RED:      Write a failing test (test MUST fail first)
GREEN:    Write MINIMAL code to pass (nothing more)
REFACTOR: Improve code while keeping tests green
REPEAT:   Next feature/scenario
```

## When to Use

- Implementing new features
- Adding new queue types or event handlers
- Fixing bugs (write test that reproduces bug FIRST)
- Refactoring existing code
- Modifying the ActiveJob adapter
- Changing worker/process behavior
- Adding dashboard endpoints

## Workflow

### Step 1: Write Failing Tests (RED)

```ruby
# spec/pgbus/example_spec.rb
RSpec.describe Pgbus::NewFeature do
  describe "#process" do
    context "when message is valid" do
      it "processes successfully" do
        expect(subject.process(message)).to eq(:success)
      end
    end

    context "when max retries exceeded" do
      it "routes to dead letter queue" do
        expect(subject.process(stale_message)).to eq(:dead_lettered)
      end
    end
  end
end
```

### Step 2: Run Tests - Verify FAIL

```bash
bundle exec rspec spec/pgbus/example_spec.rb

FAIL - NotImplementedError / Expected behavior not met
```

**Tests MUST fail before implementing.** This confirms:
- Tests are actually running
- Tests are testing the right thing
- Implementation doesn't already exist

### Step 3: Implement Minimal Code (GREEN)

Write the minimum code to make the test pass.

### Step 4: Run Tests - Verify PASS

```bash
bundle exec rspec spec/pgbus/example_spec.rb

N examples, 0 failures
```

### Step 5: Refactor (IMPROVE)

Improve code while keeping tests green:
- Extract methods to reduce complexity
- Improve naming
- Reduce duplication
- Ensure thread safety

### Step 6: Run Full Suite

```bash
bundle exec rspec
```

## Coverage Requirements

| Code Type | Minimum Coverage |
|-----------|------------------|
| All code | 80% |
| ActiveJob adapter | 100% |
| Event bus (handler, registry) | 100% |
| Process model (worker, supervisor) | 100% |
| Client (PGMQ wrapper) | 100% |
| Web::DataSource | 100% |
| Configuration | 100% |

## Test Types to Include

### Unit Tests (Configuration, Event, Serializer)
- Happy path scenarios
- Edge cases (nil values, empty queues, expired VT)
- Error conditions

### Integration Tests (Client, Adapter, Worker)
- ActiveJob enqueue/execute lifecycle
- Worker claim-process-archive flow
- Dead letter queue routing
- Event publish/subscribe round-trip

### Web Tests (DataSource, Authentication)
- DataSource with mocked PGMQ client
- Authentication block allow/deny
- Helper formatting

## Best Practices

**DO:**
- Write the test FIRST, before any implementation
- Run tests and verify they FAIL before implementing
- Write MINIMAL code to make tests pass
- Refactor only after tests are green
- Mock PGMQ client in unit tests (avoid DB dependency)
- Test worker recycling thresholds explicitly

**DON'T:**
- Write implementation before tests
- Skip running tests after each change
- Write too much code at once
- Ignore failing tests
- Test implementation details (test behavior)
- Skip testing error paths

## Checklist

- [ ] Tests written BEFORE implementation
- [ ] Tests fail initially (RED phase verified)
- [ ] Minimal code written to pass (GREEN)
- [ ] Code refactored with tests still passing
- [ ] Coverage meets requirements (80%+)
- [ ] All edge cases covered
- [ ] Backwards compatibility maintained
