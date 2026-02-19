import time
import os
import struct
import hashlib
import shutil
from NexusLogEngine import ShardedNexusLogEngine
from NexusIndex import DynamicNexusIndex
from NexusWAL import NexusWAL, WAL_STATE
from NexusLock import NexusLock



class NexusCore:
    def __init__(self, base_dir="nexus_db", initial_buckets=1_000_000):
        self.base_dir = base_dir
        if not os.path.exists(self.base_dir): os.makedirs(self.base_dir)
        
        self.index = DynamicNexusIndex(os.path.join(self.base_dir, "nexus.idx"))
        self.storage = ShardedNexusLogEngine(os.path.join(self.base_dir, "storage"))
        self.wal = NexusWAL(os.path.join(self.base_dir, "nexus.wal"))
        self.index_lock = NexusLock(self.index.f)

        self.last_lsn = 0
        self._recover()

    def _recover(self):
        all_entries = self.wal.read_all()
        # 인덱스 헤더에서 마지막으로 안전하게 Flush된 LSN 확인
        checkpoint_lsn = self.index.get_last_lsn()
        
        prepares = {}
        commits = set()
        resize_in_progress = False

        for ent in all_entries:
            lsn, state, u_hash, off, length, ts, shard = ent[:7]
            
            # Checkpoint LSN보다 작거나 같은 기록은 이미 반영된 것이므로 무시
            if lsn <= checkpoint_lsn:
                continue

            if state == WAL_STATE.RESIZE_START:
                resize_in_progress = True
            elif state == WAL_STATE.RESIZE_END:
                resize_in_progress = False
            elif state == WAL_STATE.PREPARE:
                prepares[lsn] = (u_hash, off, length, ts, shard)
            elif state == WAL_STATE.COMMIT:
                commits.add(lsn)

        # 1. Resize 도중 크래시 났다면 .tmp 파일 삭제 (원자성 보장)
        if resize_in_progress:
            tmp_path = self.index.index_path + ".tmp"
            if os.path.exists(tmp_path): os.remove(tmp_path)
            print("[!] Detected crash during resize. Rolled back.")

        # 2. 미확정 트랜잭션 Redo (Checkpoint 이후 항목만)
        recovered = 0
        for lsn, data in prepares.items():
            if lsn not in commits:
                self.index._raw_insert(*data)
                recovered += 1
        
        if recovered > 0:
            self.index.update_header(lsn=max(prepares.keys()))
            self.index.flush_to_disk()

        # 3. 마지막 LSN 갱신 및 WAL 관리
        self.last_lsn = max([e[0] for e in all_entries]) if all_entries else checkpoint_lsn
        
        # WAL이 너무 크면 Checkpoint 후 비우기
        if len(all_entries) > 10000:
            self.wal.checkpoint()

    def _resize_wal_callback(self, start=True, new_size=0):
        """인덱스 리사이즈 시 WAL에 트랜잭션 경계 기록"""
        state = WAL_STATE.RESIZE_START if start else WAL_STATE.RESIZE_END
        self.last_lsn += 1
        # 리사이즈 마커는 데이터가 없으므로 0/empty 값으로 기록
        self.wal.append(state, b"\0"*16, new_size, 0, 0, 0, self.last_lsn)

    def put(self, url, content, metadata=None):
        
        with self.index_lock.exclusive():
            if (self.index.used_count / self.index.bucket_count) > 0.7:
                self.index._resize(core_callback=self._resize_wal_callback)

        with self.index_lock.exclusive():
            self.last_lsn += 1
            u_hash = hashlib.sha256(url.encode()).digest()[:16]
            
            try:
                # 1. 스토리지 로그 기록 (데이터 본문)
                log_info = self.storage.append_record(url, content, metadata=metadata)
                s_id = int(log_info['shard_id'], 16)

                # 2. WAL PREPARE (인덱스 쓰기 전 의도 기록)
                self.wal.append(WAL_STATE.PREPARE, u_hash, log_info['offset'], 
                                log_info['length'], log_info['timestamp'], s_id, self.last_lsn)

                # 3. 인덱스 기록
                self.index._raw_insert(u_hash, log_info['offset'], log_info['length'], 
                                    log_info['timestamp'], s_id)

                # 5. 주기적 또는 매번 Checkpoint LSN 업데이트
                self.index.update_header(lsn=self.last_lsn)
                
                # 4. 인덱스 물리적 Flush (가장 중요)
                self.index.flush_to_disk()


                # 6. COMMIT 기록
                self.wal.append(WAL_STATE.COMMIT, u_hash, 0, 0, 0, 0, self.last_lsn)
                
                return log_info['url_hash']
            except Exception as e:
                print(f"[-] Put failed: {e}")
                return None

    def get(self, url):
        """
        Shard ID를 인덱스에서 직접 읽어 데이터를 찾아옴
        """
        with self.index_lock.shared():
            idx_entry = self.index.lookup(url) # 수정된 lookup은 shard_id도 반환해야 함
            if not idx_entry:
                return None
                
            # 인덱스에 저장된 shard_id를 사용하여 파일 경로 결정 (역산 필요 없음)
            shard_id_str = f"{idx_entry['shard_id']:02x}"
            
            try:
                return self.storage.read_record(shard_id_str, idx_entry['offset'])
            except Exception as e:
                print(f"[-] Get Error: {e}")
                return None

    def close(self):
        self.index.close()

# --- 작동 확인 테스트 ---
if __name__ == "__main__":
    core = NexusCore(base_dir="my_local_search_db")

    # 데이터 저장 테스트
    test_url2 = "https://ollama.com/"
    # test_url = "https://ollama.com/library/llama3"
    # test_content = "Llama 3 is the next generation of large language models..."
    
    # print("[*] 데이터 저장 중...")
    # hash_result = core.put(test_url, test_content, metadata={"category": "AI", "priority": 1})
    
    # if hash_result:
    #     print(f"[+] 저장 성공! 해시: {hash_result}")

    # test_url = "https://ollama.com/"
    # test_content = "Llama 3"
    
    # print("[*] 데이터 저장 중...")
    # hash_result = core.put(test_url, test_content, metadata={"category": "AI", "priority": 1})
    
    # if hash_result:
    #     print(f"[+] 저장 성공! 해시: {hash_result}")
    
    # 데이터 검색 테스트
    print("\n[*] 데이터 검색 중...")
    retrieved = core.get(test_url2)
    
    if retrieved:
        print(f"[+] 검색 결과 발견!")
        print(f"내용 요약: {retrieved['content'][:50]}...")
        print(f"메타데이터: {retrieved['metadata']}")
    else:
        print("[-] 데이터를 찾을 수 없습니다.")

    core.close()