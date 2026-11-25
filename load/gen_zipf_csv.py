#!/usr/bin/env python3
"""Generate the login key list (registration_no, dob) for the load scripts.

Keys are drawn with a bounded Zipf distribution over the N loaded candidates, so a
minority of candidates is hit far more often than the rest (people who refresh, or
whose exam was just published). The rank -> candidate mapping is a seeded random
permutation, so "hot" candidates are scattered across the table instead of being
ids 1..k packed into a few pages.

registration_no and dob are computed with the SAME rules as db/01_schema.sql
(rrb.registration_no(), rrb.dob()), so no export from the database is needed.

Usage:
    python3 gen_zipf_csv.py --n 15000000 --rows 3000000 --s 0.7 --out keys.csv
    python3 gen_zipf_csv.py --n 15000000 --s 1.1 ...     # exponent written in the proposal

Choosing --s (stats are printed for the file you generate):
    s=1.1 over 1.5 crore keys: the single hottest candidate gets ~11.5% of ALL logins and
          1% of candidates get ~87%: the hot set fits in cache, which flatters the DB.
    s=0.7: hottest candidate ~0.2%, top 1% ~25%, ~75% of draws are distinct. Default.
    s=0   : uniform (worst case for the buffer cache).

--rows: the load tools walk the file in order and wrap around. Make it at least the
number of sessions in the longest run you care about NOT repeating, e.g.
25,000/s x 600 s = 1.5 crore for a fully non-repeating 10-minute spike; 30 lakh rows
(~75 MB) is a practical default.

--bad-pct: percentage of rows with a wrong DOB (failed logins -> audit FAIL rows).
"""
import argparse
import datetime as dt
import sys
import time

import numpy as np


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=15_000_000, help="candidates loaded in the DB (default 1.5 crore)")
    ap.add_argument("--rows", type=int, default=3_000_000, help="rows to write (default 30 lakh)")
    ap.add_argument("--s", type=float, default=0.7, help="Zipf exponent; 0 = uniform (default 0.7)")
    ap.add_argument("--bad-pct", type=float, default=0.0, help="percent of rows with a wrong DOB (default 0)")
    ap.add_argument("--seed", type=int, default=20260115)
    ap.add_argument("--out", default="keys.csv")
    a = ap.parse_args()

    if a.n < 1 or a.rows < 1 or a.s < 0 or not 0 <= a.bad_pct <= 100:
        ap.error("invalid arguments")

    t0 = time.time()
    rng = np.random.default_rng(a.seed)

    # Bounded Zipf by inverse CDF over ranks 1..N (numpy's zipf() is unbounded and needs s > 1).
    ranks = np.arange(1, a.n + 1, dtype=np.float64)
    cdf = np.cumsum(ranks ** -a.s)
    cdf /= cdf[-1]
    rank_idx = np.searchsorted(cdf, rng.random(a.rows), side="right")
    np.minimum(rank_idx, a.n - 1, out=rank_idx)

    # Scatter ranks over candidate ids.
    perm = rng.permutation(a.n) + 1                          # candidate_id for each rank
    ids = perm[rank_idx].astype(np.int64)
    del perm

    # Same formulas as rrb.dob() / rrb.registration_no().
    epoch = np.datetime64("1990-01-01")
    dob = epoch + ((ids * 7919) % 5844).astype("timedelta64[D]")
    if a.bad_pct > 0:
        bad = rng.random(a.rows) < a.bad_pct / 100.0
        dob[bad] = dob[bad] - np.timedelta64(1, "D")         # off by one day: realistic typo
    else:
        bad = np.zeros(a.rows, dtype=bool)

    regno = np.char.add("RRB", np.char.zfill(ids.astype(str), 10))
    dob_s = dob.astype(str)
    with open(a.out, "w", encoding="ascii", newline="\n") as f:
        f.write("registration_no,dob,candidate_id\n")
        for chunk in range(0, a.rows, 500_000):
            sl = slice(chunk, chunk + 500_000)
            f.write("\n".join(f"{r},{d},{i}" for r, d, i in zip(regno[sl], dob_s[sl], ids[sl])))
            f.write("\n")

    # Stats so the chosen skew is visible in the run log.
    _, counts = np.unique(ids, return_counts=True)
    counts.sort()
    top1pct = counts[-max(1, len(counts) // 100):].sum() / a.rows * 100
    top1pct_keys = cdf[max(0, a.n // 100 - 1)] * 100
    print(f"wrote {a.rows:,} rows to {a.out} in {time.time() - t0:.1f}s", file=sys.stderr)
    print(f"  N={a.n:,}  s={a.s}  seed={a.seed}  bad DOB rows={bad.sum():,}", file=sys.stderr)
    print(f"  distinct candidates in file : {len(counts):,} ({len(counts) / a.rows * 100:.1f}% of rows)", file=sys.stderr)
    print(f"  hottest candidate           : {counts[-1]:,} rows ({counts[-1] / a.rows * 100:.2f}%)", file=sys.stderr)
    print(f"  top 1% of N (by rank) share : {top1pct_keys:.1f}% of draws (theoretical)", file=sys.stderr)
    print(f"  top 1% of distinct in file  : {top1pct:.1f}% of rows", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
