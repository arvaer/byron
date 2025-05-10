#!/bin/bash
set -exuo pipefail

# ---- config ----
WORKLOAD_FILE="./workload.txt"
ENTRY_SIZE_BYTES=8   # 8 bytes key + 8 bytes value

# grab byorn server PID
PID=$(pgrep -x byron_server)
if [ -z "$PID" ]; then
    echo "Byron server not running"
    exit 1
fi
echo "Profiling byron from server PID=$PID"

if [ $# -lt 1 ]; then
  echo "Usage: $0 <your_command> [args...]"
  exit 1
fi
CMD=( "$@" )
echo "Command is $CMD"

# ---- 1) compute workload statistics ----
PUTS=$(grep -c '^p ' "$WORKLOAD_FILE" || true)
GETS=$(grep -c '^g ' "$WORKLOAD_FILE" || true)
DELETES=$(grep -c '^d ' "$WORKLOAD_FILE" || true)
RANGES=$(grep -c '^r ' "$WORKLOAD_FILE" || true)

DATA_SIZE_BYTES=$(( PUTS * ENTRY_SIZE_BYTES ))
DATA_SIZE_HR=$(numfmt --to=iec --suffix=B "$DATA_SIZE_BYTES")

echo "=== Workload summary ==="
echo "  puts:    $PUTS"
echo "  gets:    $GETS"
echo "  deletes: $DELETES"
echo "  ranges:  $RANGES"
echo "  estimated dataset size: $DATA_SIZE_HR ($DATA_SIZE_BYTES bytes)"
echo

# ---- 2) setup logs ----
mkdir -p ./log
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
PERF_LOG="./log/perf_${TIMESTAMP}.log"
IOSTAT_LOG="./log/iostat_${TIMESTAMP}.log"
BPF_LOG="./log/iobpf_${TIMESTAMP}.log"
CLIENT_OUT="./log/client_${TIMESTAMP}.out"

echo "Logs will be written to:"
echo "  $PERF_LOG   (perf stat)"
echo "  $IOSTAT_LOG (iostat)"
echo "  $BPF_LOG    (bpftrace)"
echo "  $CLIENT_OUT (your program stdout)"
echo

# ---- 3) start background metrics ----
# 3.1 iostat
iostat -dx 1 > "$IOSTAT_LOG" &
IOSTAT_PID=$!

# 3.2 eBPF for block I/O counts (requires sudo)
sudo bpftrace -p "$PID" -e '
  tracepoint:block:block_rq_issue { @[args->rwbs]++ }
' > "$BPF_LOG" &
BPF_PID=$!

# ---- 4) perf stat ----
echo "Running command under perf stat..."
perf stat -e cycles,instructions,cache-references,cache-misses -p "$PID"\
 -- "${CMD[@]}" \
  2> "$PERF_LOG" \
  > "$CLIENT_OUT"
STATUS=$?

# ---- 5) clean up ----
echo "Benchmark finished (exit code $STATUS)."
kill $IOSTAT_PID 2>/dev/null || true
sudo kill $BPF_PID     2>/dev/null || true

echo
echo "All logs written."
echo "  perf:    $PERF_LOG"
echo "  iostat:  $IOSTAT_LOG"
echo "  bpftrace:$BPF_LOG"
echo "  output:  $CLIENT_OUT"

exit $STATUS

