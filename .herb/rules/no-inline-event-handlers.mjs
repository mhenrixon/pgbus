// Custom Herb rule: no-inline-event-handlers
//
// Flags HTML attributes whose names look like inline event handlers
// (onclick, onchange, onsubmit, …). These attributes are blocked by
// strict CSPs (script-src-attr) and silently break the dashboard for
// any host app whose CSP doesn't include 'unsafe-inline' for attrs.
//
// Bug history: pgbus 0.4.0 shipped onclick/onchange handlers in the
// dashboard layout that broke under zazu.ma's CSP, leaving the locale
// switcher, dark-mode toggle, and mobile menu inert. This rule
// prevents that class of regression — bind events from a nonced
// <script> via addEventListener instead.

import {
  ParserRule,
  BaseRuleVisitor,
  getAttributes,
  getAttributeName,
} from "@herb-tools/linter";

const EVENT_HANDLER_PATTERN = /^on[a-z]+$/;

class NoInlineEventHandlersVisitor extends BaseRuleVisitor {
  visitHTMLOpenTagNode(node) {
    for (const attribute of getAttributes(node)) {
      const name = getAttributeName(attribute);
      if (!name || !EVENT_HANDLER_PATTERN.test(name)) continue;

      this.addOffense(
        `Avoid inline event handler \`${name}="…"\`. Strict Content-Security-Policies block these attributes (script-src-attr). ` +
          `Bind the listener from a nonced <script> via \`addEventListener\` and trigger it with a \`data-*\` attribute instead.`,
        attribute.location,
      );
    }
    super.visitHTMLOpenTagNode(node);
  }
}

export default class NoInlineEventHandlersRule extends ParserRule {
  static ruleName = "no-inline-event-handlers";
  static introducedIn = "0.9.5";

  get defaultConfig() {
    return {
      enabled: true,
      severity: "error",
    };
  }

  check(result, context) {
    const visitor = new NoInlineEventHandlersVisitor(this.ruleName, context);
    visitor.visit(result.value);
    return visitor.offenses;
  }
}
