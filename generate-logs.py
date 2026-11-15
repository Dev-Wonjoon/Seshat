import argparse
import csv
import json
import time
from pathlib import Path

NANOSECONDS = 1_000_000_000


def make_line(sequence, size_bytes):
    prefix = f'{{"sequence":{sequence},"message":"'.encode("utf-8")
    suffix = b'"}\n'
    padding = size_bytes - len(prefix) - len(suffix)
    if padding < 0:
        raise ValueError("지정한 크기에 JSON 필드를 담을 수 없습니다.")
    return prefix + b"x" * padding + suffix


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log", type=Path)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--bytes", type=int, default=512)
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=30)
    parser.add_argument("--duration", type=int, default=60)
    args = parser.parse_args()

    if args.rate < 1 or args.duration < 1 or args.warmup < 0:
        parser.error("발생률과 측정 시간은 양수, 워밍업은 0 이상이어야 합니다.")
    if args.log.resolve() == args.csv.resolve():
        parser.error("로그와 CSV에는 서로 다른 경로를 지정하세요.")

    warmup_slots = args.warmup * args.rate
    total_slots = (args.warmup + args.duration) * args.rate
    if args.bytes < len(make_line(total_slots, 128).rstrip(b"x")):
        # 최대 순번을 사용해 실제 최소 길이를 확인한다.
        minimum = len(f'{{"sequence":{total_slots},"message":""}}\n'.encode("utf-8"))
        if args.bytes < minimum:
            parser.error(f"레코드 크기는 최소 {minimum}바이트여야 합니다.")

    counts = {"warmup": 0, "measure": 0}
    max_lateness_ns = 0

    # 에이전트가 미리 열어 둔 빈 파일에 줄 단위로 바로 쓴다.
    with args.log.open("ab", buffering=0) as log:
        if log.tell() != 0:
            parser.error("실험마다 비어 있는 새 로그 파일을 지정하세요.")

        with args.csv.open("x", encoding="utf-8", newline="") as manifest:
            writer = csv.writer(manifest)
            writer.writerow(["sequence", "phase", "generated_at_ns", "bytes"])

            start = time.monotonic_ns()
            slot = 0

            while slot < total_slots:
                deadline = start + slot * NANOSECONDS // args.rate
                now = time.monotonic_ns()
                if now < deadline:
                    time.sleep((deadline - now) / NANOSECONDS)
                    continue

                # 밀린 슬롯을 한꺼번에 기록하면 실제 발생률이 순간적으로 튄다.
                latest_slot = (now - start) * args.rate // NANOSECONDS
                slot = max(slot, latest_slot)
                if slot >= total_slots:
                    break

                sequence = slot + 1
                phase = "warmup" if slot < warmup_slots else "measure"
                line = make_line(sequence, args.bytes)

                generated_at_ns = time.time_ns()
                if log.write(line) != len(line):
                    raise OSError("NDJSON 한 줄을 끝까지 쓰지 못했습니다.")

                writer.writerow([sequence, phase, generated_at_ns, len(line)])
                counts[phase] += 1
                scheduled = start + slot * NANOSECONDS // args.rate
                max_lateness_ns = max(max_lateness_ns, now - scheduled)
                slot += 1

    expected = args.duration * args.rate
    print(json.dumps({
        "warmup_written": counts["warmup"],
        "measure_written": counts["measure"],
        "measure_expected": expected,
        "measure_missed": expected - counts["measure"],
        "measure_actual_rate": counts["measure"] / args.duration,
        "max_schedule_lateness_ms": max_lateness_ns / 1_000_000,
    }))


if __name__ == "__main__":
    main()