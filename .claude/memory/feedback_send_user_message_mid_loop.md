---
name: feedback_send_user_message_mid_loop
description: Mid-loop replies to the user must go via the SendUserMessage tool — plain assistant text between tool calls doesn't render in their terminal
type: feedback
---

When the user asks a question mid-task — especially during a /loop or
ScheduleWakeup-driven drain — answer via the **SendUserMessage tool**, not
plain assistant text emitted between tool calls.

**Why:** During the CHA-427 drain (2026-06-12), two consecutive plain-text
answers to a user question never rendered in their terminal; the user saw
nothing and asked again. The same content delivered through SendUserMessage
rendered immediately. Loop ticks end in a ScheduleWakeup call, and text
woven between tool calls in those turns is not reliably displayed.

**How to apply:** If a turn both does drain work (tool calls ending in
ScheduleWakeup) and answers a user question, put the answer in a
SendUserMessage call (status "normal" for direct replies). Plain final-text
responses remain fine for ordinary turns that end without a wakeup/tool
tail; when in doubt mid-loop, prefer SendUserMessage.
