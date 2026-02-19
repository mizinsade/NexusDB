import os
import shutil
import hashlib
import random
import struct
import time

from NexusCore import NexusCore

DB_DIR = "test_nexus_db"


def reset_db():
    if os.path.exists(DB_DIR):
        shutil.rmtree(DB_DIR)


def reopen_db():
    return NexusCore(base_dir=DB_DIR)


def simulate_partial_wal_write():
    """
    WAL entry 일부만 기록된 상황 시뮬레이션
    """
    wal_path = os.path.join(DB_DIR, "nexus.wal")
    with open(wal_path, "ab") as f:
        f.write(b"\x01\x02\x03")  # 깨진 엔트리


def simulate_duplicate_wal_entry(url):
    """
    동일 WAL entry 2번 기록
    """
    core = reopen_db()
    log_info = core.storage.append_record(url, "dup data")
    u_hash = hashlib.sha256(url.encode()).digest()[:16]
    s_id = int(log_info['shard_id'], 16)

    lsn = int(time.time() * 1000000)  # 간단한 LSN 생성

    core.wal.append(1, u_hash,
                    log_info['offset'],
                    log_info['length'],
                    log_info['timestamp'],
                    s_id,
                    lsn)

    core.wal.append(1, u_hash,
                    log_info['offset'],
                    log_info['length'],
                    log_info['timestamp'],
                    s_id,
                    lsn + 1)
    del core


def simulate_index_only_write(url):
    """
    WAL 없이 index만 기록된 상태
    """
    core = reopen_db()
    log_info = core.storage.append_record(url, "index_only")
    u_hash = hashlib.sha256(url.encode()).digest()[:16]
    s_id = int(log_info['shard_id'], 16)

    core.index._raw_insert(
        u_hash,
        log_info['offset'],
        log_info['length'],
        log_info['timestamp'],
        s_id
    )
    del core


def simulate_resize_stress():
    """
    resize 강제 발생
    """
    core = reopen_db()
    for i in range(100000):
        print(i)
        core.put(f"https://resize.com/{i}", "X" * 10)
    core.close()


def simulate_mass_insert(count=5000):
    core = reopen_db()
    for i in range(count):
        core.put(f"https://mass.com/{i}", f"DATA-{i}")
    core.close()


def simulate_update_same_key():
    core = reopen_db()
    url = "https://update.com/key"
    core.put(url, "version1")
    core.put(url, "version2")
    core.close()


def simulate_random_crash_points():
    core = reopen_db()

    url = "https://random-crash.com/test"
    content = "random crash data"

    log_info = core.storage.append_record(url, content)
    u_hash = hashlib.sha256(url.encode()).digest()[:16]
    s_id = int(log_info['shard_id'], 16)
    lsn = int(time.time() * 1000000)
    crash_point = random.choice([
        "after_log",
        "after_wal",
        "after_index",
        "after_wal_clear"
    ])

    if crash_point == "after_log":
        print("[CRASH] after storage log")
        del core
        return

    core.wal.append(1, u_hash, log_info['offset'],
                    log_info['length'], log_info['timestamp'], s_id, lsn)

    if crash_point == "after_wal":
        print("[CRASH] after WAL append")
        del core
        return

    core.index._raw_insert(
        u_hash,
        log_info['offset'],
        log_info['length'],
        log_info['timestamp'],
        s_id
    )

    if crash_point == "after_index":
        print("[CRASH] after index write")
        del core
        return

    # core.wal.clear()

    if crash_point == "after_wal_clear":
        print("[CRASH] after WAL clear")
        del core
        return

    core.close()


def verify_all():
    core = reopen_db()
    errors = 0

    test_urls = [
        "https://update.com/key",
        "https://random-crash.com/test"
    ]

    for i in range(5000):
        test_urls.append(f"https://mass.com/{i}")

    for url in test_urls:
        try:
            result = core.get(url)
            if result:
                pass
        except Exception as e:
            print("[ERROR]", url, e)
            errors += 1

    core.close()
    return errors


def full_engine_test():
    reset_db()

    print("\n[1] 정상 insert")
    core = reopen_db()
    core.put("https://normal.com", "normal")
    core.close()

    print("\n[2] WAL 부분 손상 테스트")
    simulate_partial_wal_write()

    print("\n[3] 중복 WAL 엔트리")
    simulate_duplicate_wal_entry("https://dup.com")

    print("\n[4] index only 기록")
    simulate_index_only_write("https://indexonly.com")

    print("\n[5] 대량 insert")
    simulate_mass_insert(2000)

    print("\n[6] 업데이트 테스트")
    simulate_update_same_key()

    print("\n[7] 랜덤 크래시 반복")
    for _ in range(10):
        simulate_random_crash_points()

    print("\n[8] resize 스트레스")
    simulate_resize_stress()

    print("\n[9] 최종 검증")
    errors = verify_all()

    if errors == 0:
        print("\n✔ 엔진 테스트 통과 (치명적 예외 없음)")
    else:
        print(f"\n❌ 에러 {errors}개 발견")


if __name__ == "__main__":
    full_engine_test()
