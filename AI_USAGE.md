# AI usage statement (required by the assignment)

| Tool | Purpose | Where |
|---|---|---|
| Claude (Anthropic), chat interface | Drafted the initial repository scaffold: generator, Spark pipeline, detection/forecast/optimiser/calendar modules, serving export, D3 dashboard, tests, Makefile, and the draft report/proposal text | whole repository |

Representative prompts (edit to match what YOUR team actually did):
1. "complete the whole assignment and give me a word file of that" (uploaded the CEP brief PDF)
2. "explain what to do ... distribute it on 3 people ... step by step process"
3. "can you give a zip folder of the full assignment project"

What the AI ran/verified in its sandbox: generator at SF=0.01, bronze/silver/gold, analytics, serving export, unit tests, a jsdom smoke test
of the dashboard. What it did **not** do: any run at SF >= 0.25, the 45-minute check, real weather/boundary data, browser visual testing,
usability tests, Spark UI screenshots. Numbers in `docs/RESULTS_SANDBOX.md` are from that SF=0.01 sandbox run only.

**Team obligation:** every member must be able to explain every file during an authorship verification. Replace this paragraph with
your own account of what each member changed, debugged and re-ran. Code you cannot explain earns no credit.
