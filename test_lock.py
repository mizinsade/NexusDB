import multiprocessing
import time
import os
import shutil
from NexusCore import NexusCore

def worker_write(process_id, base_dir, count):
    """각 프로세스가 고유한 데이터를 DB에 저장"""
    # 각 프로세스는 독립적인 핸들을 가져야 하므로 내부에서 생성
    core = NexusCore(base_dir=base_dir)
    success_count = 0
    
    for i in range(count):
        url = f"https://example.com/proc_{process_id}/page_{i}"
        content = f"Content from process {process_id}, index {i}. " * 10
        meta = {"proc": process_id, "idx": i}
        
        res = core.put(url, content, metadata=meta)
        if res:
            success_count += 1
            
    core.close()
    print(f"[Worker {process_id}] Finished writing {success_count}/{count} records.")

def worker_read(process_id, base_dir, count):
    """저장된 데이터를 무작위로 읽어 무결성 확인"""
    core = NexusCore(base_dir=base_dir)
    found_count = 0
    
    # 쓰기 프로세스가 어느 정도 진행될 때까지 대기
    time.sleep(1)
    
    for i in range(count):
        # 다른 프로세스가 썼을 법한 URL 생성
        target_proc = i % 4 # 0~3번 프로세스 데이터 타겟
        url = f"https://example.com/proc_{target_proc}/page_{i // 4}"
        
        retrieved = core.get(url)
        if retrieved:
            found_count += 1
            
    core.close()
    print(f"[Reader {process_id}] Finished reading. Found {found_count} records.")

if __name__ == "__main__":
    DB_DIR = "parallel_test_db"
    
    # 1. 기존 테스트 데이터 삭제
    if os.path.exists(DB_DIR):
        shutil.rmtree(DB_DIR)
    
    # 2. 프로세스 설정 (4개는 쓰고, 2개는 읽기)
    num_writers = 4
    num_readers = 2
    items_per_writer = 500  # 총 2,000개 데이터
    
    processes = []
    
    print(f"[*] Starting Parallel Test (Writers: {num_writers}, Readers: {num_readers})...")
    start_time = time.time()

    # 쓰기 프로세스 시작
    for i in range(num_writers):
        p = multiprocessing.Process(target=worker_write, args=(i, DB_DIR, items_per_writer))
        processes.append(p)
        p.start()

    # 읽기 프로세스 시작
    for i in range(num_readers):
        p = multiprocessing.Process(target=worker_read, args=(i + 10, DB_DIR, items_per_writer))
        processes.append(p)
        p.start()

    # 모든 프로세스 종료 대기
    for p in processes:
        p.join()

    end_time = time.time()
    print("-" * 40)
    print(f"[*] Test Completed in {end_time - start_time:.2f} seconds.")
    
    # 3. 최종 무결성 확인
    final_core = NexusCore(base_dir=DB_DIR)
    print(f"[*] Final Check: Used Buckets = {final_core.index.used_count}")
    final_core.close()