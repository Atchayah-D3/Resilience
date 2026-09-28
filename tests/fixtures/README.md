# Decomposer fixtures

Recorded event streams for the pure analysis functions (Arch §7.3, §13). A decomposer bug
produces plausible-looking wrong numbers rather than crashing, so it is tested against
recorded streams.

`power_loss_standalone.jsonl` is synthetic, built to the shape the harness emits: first
successful write 31.4 s after T0, first compliant 60 s SLO window starting at 36 s.
