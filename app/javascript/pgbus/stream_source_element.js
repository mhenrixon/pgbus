// <pgbus-stream-source> — the browser-side counterpart to
// Pgbus::StreamsHelper#pgbus_stream_from. Drop-in replacement for
// turbo-rails' <turbo-cable-stream-source> that speaks SSE + pgbus
// instead of WebSocket + ActionCable.
//
// Attributes (set by the view helper):
//   src                 — absolute path to the SSE endpoint, including
//                         the URL-safe signed stream name
//   since-id            — PGMQ msg_id watermark at render time; the
//                         client sends this as ?since= on the FIRST
//                         connect so the server can replay any
//                         broadcasts from the render-to-connect gap
//                         (rails/rails#52420 fix)
//   signed-stream-name  — present for parity with turbo-rails; unused
//                         by this element because the signed name is
//                         already in the URL path
//   channel             — compatibility shim; ignored
//
// Events (dispatched on the element):
//   pgbus:open         { lastEventId }
//   pgbus:replay-start { fromId, toId }
//   pgbus:replay-end   {}
//   pgbus:gap-detected { lastSeenId, archiveOldestId }
//   pgbus:close        { code, reason }
//
// The element integrates with Turbo via connectStreamSource /
// disconnectStreamSource + dispatching MessageEvent("message") so Turbo
// Stream HTML is automatically consumed by the existing StreamObserver.
//
// Transport strategy:
//   - FIRST connect: use fetch() + ReadableStream to include ?since=
//     in the URL. Native EventSource cannot send Last-Event-ID on the
//     initial request, so we need fetch to carry the watermark.
//   - RECONNECT: switch to native EventSource, which sends Last-Event-ID
//     automatically based on the last id: we observed. The native
//     client is more battle-tested for reconnection backoff.

import { connectStreamSource, disconnectStreamSource } from "@hotwired/turbo"

class PgbusStreamSourceElement extends HTMLElement {
  static get observedAttributes() {
    return ["src", "since-id"]
  }

  constructor() {
    super()
    this.abortController = null
    this.eventSource = null
    this.lastEventId = null
    this.closed = false
  }

  connectedCallback() {
    connectStreamSource(this)
    const sinceId = this.getAttribute("since-id")
    this.lastEventId = sinceId && sinceId !== "" ? sinceId : null
    this.openFetchStream()
  }

  disconnectedCallback() {
    this.closed = true
    disconnectStreamSource(this)
    this.teardown()
  }

  teardown() {
    if (this.abortController) {
      this.abortController.abort()
      this.abortController = null
    }
    if (this.eventSource) {
      this.eventSource.close()
      this.eventSource = null
    }
  }

  // First connect: use fetch() so we can include ?since=<watermark> on
  // the URL. Parses the SSE event stream by hand because EventSource
  // doesn't expose custom query strings uniformly across browsers.
  async openFetchStream() {
    const url = this.buildUrl({ includeSince: true })
    this.abortController = new AbortController()

    try {
      const response = await fetch(url, {
        headers: { Accept: "text/event-stream" },
        credentials: "same-origin",
        signal: this.abortController.signal
      })

      if (!response.ok) {
        this.dispatchEvent(new CustomEvent("pgbus:close", {
          detail: { code: response.status, reason: response.statusText }
        }))
        return
      }

      this.setAttribute("connected", "")
      this.dispatchEvent(new CustomEvent("pgbus:open", {
        detail: { lastEventId: this.lastEventId }
      }))

      const reader = response.body.getReader()
      const decoder = new TextDecoder("utf-8")
      let buffer = ""

      while (!this.closed) {
        const { value, done } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const events = buffer.split("\n\n")
        buffer = events.pop() // last chunk may be incomplete

        for (const block of events) {
          this.handleBlock(block)
        }
      }
    } catch (err) {
      if (err.name !== "AbortError") {
        this.removeAttribute("connected")
        // Fall through to reconnect via native EventSource, which
        // will carry Last-Event-ID from this.lastEventId forward.
        this.switchToEventSource()
      }
    }
  }

  // Reconnect path: native EventSource with Last-Event-ID baked into
  // the initial handshake by the browser.
  switchToEventSource() {
    if (this.closed) return

    const url = this.buildUrl({ includeSince: false })
    this.eventSource = new EventSource(url, { withCredentials: true })

    this.eventSource.addEventListener("open", () => {
      this.setAttribute("connected", "")
      this.dispatchEvent(new CustomEvent("pgbus:open", {
        detail: { lastEventId: this.lastEventId }
      }))
    })

    this.eventSource.addEventListener("error", () => {
      this.removeAttribute("connected")
    })

    this.eventSource.addEventListener("turbo-stream", (event) => {
      this.lastEventId = event.lastEventId
      this.dispatchEvent(new MessageEvent("message", { data: event.data }))
    })

    this.eventSource.addEventListener("pgbus:gap-detected", (event) => {
      const detail = this.safeJsonParse(event.data)
      this.dispatchEvent(new CustomEvent("pgbus:gap-detected", { detail }))
    })

    this.eventSource.addEventListener("pgbus:shutdown", () => {
      this.dispatchEvent(new CustomEvent("pgbus:close", {
        detail: { code: "shutdown", reason: "worker restart" }
      }))
    })
  }

  // Parses a single SSE event block (: comment | id: ... | event: ... | data: ...)
  handleBlock(block) {
    if (!block || block.startsWith(":")) return // comment

    let id = null
    let event = "message"
    let data = ""

    for (const line of block.split("\n")) {
      if (line.startsWith("id:")) id = line.slice(3).trim()
      else if (line.startsWith("event:")) event = line.slice(6).trim()
      else if (line.startsWith("data:")) data += line.slice(5).trim()
    }

    if (id !== null) this.lastEventId = id

    if (event === "turbo-stream") {
      this.dispatchEvent(new MessageEvent("message", { data }))
    } else if (event === "pgbus:gap-detected") {
      this.dispatchEvent(new CustomEvent("pgbus:gap-detected", {
        detail: this.safeJsonParse(data)
      }))
    } else if (event === "pgbus:shutdown") {
      this.dispatchEvent(new CustomEvent("pgbus:close", {
        detail: { code: "shutdown", reason: "worker restart" }
      }))
    }
  }

  buildUrl({ includeSince }) {
    const src = this.getAttribute("src")
    if (!includeSince || !this.lastEventId) return src

    const url = new URL(src, window.location.origin)
    url.searchParams.set("since", this.lastEventId)
    return url.toString()
  }

  safeJsonParse(str) {
    try { return JSON.parse(str) } catch { return null }
  }
}

if (!customElements.get("pgbus-stream-source")) {
  customElements.define("pgbus-stream-source", PgbusStreamSourceElement)
}

export { PgbusStreamSourceElement }
