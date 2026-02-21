#NexusCore.py
import time
import os
import struct
import hashlib
import shutil
from NexusLogEngine import ShardedNexusLogEngine
from NexusIndex import DynamicNexusIndex
from NexusWAL import NexusWAL, WAL_STATE
from NexusLock import NexusLock
from LSM import LSMInvertedIndex



class NexusCore:
    def __init__(self, base_dir="nexus_db", initial_buckets=1_000_000):
        self.base_dir = base_dir
        if not os.path.exists(self.base_dir): os.makedirs(self.base_dir)
        
        self.index = DynamicNexusIndex(os.path.join(self.base_dir, "nexus.idx"))
        self.storage = ShardedNexusLogEngine(os.path.join(self.base_dir, "storage"))
        self.wal = NexusWAL(os.path.join(self.base_dir, "nexus.wal"))
        self.index_lock = NexusLock(self.index.f)
        self.lsm_index = LSMInvertedIndex(os.path.join(self.base_dir, "lsm_idx"))

        self.last_lsn = 0
        self._recover()

    def _tokenize(self, text):
        return {w.lower().strip(".,! ") for w in text.split() if len(w) > 1}

    def _recover(self):
        """NexusWAL의 구조(lsn=0, state=1, hash=2, off=3, len=4, ts=5, shard=6)에 맞춤"""
        all_entries = self.wal.read_all()
        checkpoint_lsn = self.index.get_last_lsn()
        
        # 1. Analysis: ent[0]이 LSN, ent[1]이 State
        committed_lsns = {ent[0] for ent in all_entries if ent[1] == WAL_STATE.COMMIT}
        
        recovered_count = 0
        max_lsn = checkpoint_lsn

        for ent in all_entries:
            # WAL 구조에 따른 정확한 언패킹
            lsn, state, u_hash, off, length, ts, shard = ent[0], ent[1], ent[2], ent[3], ent[4], ent[5], ent[6]
            
            # 체크포인트 이전 기록은 무시
            if lsn <= checkpoint_lsn: 
                continue
            
            # 2. Redo: PREPARE 로그가 존재하고, 해당 LSN이 COMMIT 리스트에 있는 경우
            if state == WAL_STATE.PREPARE and lsn in committed_lsns:
                # KV 인덱스 복구
                self.index._raw_insert(u_hash, off, length, ts, shard)
                
                # LSM 역색인 복구
                shard_str = f"{shard:02x}"
                try:
                    data = self.storage.read_record(shard_str, off)
                    if data:
                        # LSM 복구 시 add_document를 사용하여 검색 인덱스 재구축
                        self.lsm_index.add_document(u_hash, data['content'])
                except Exception as e:
                    print(f"[-] Recovery failed to read record for LSM at LSN {lsn}: {e}")
                
                max_lsn = max(max_lsn, lsn)
                recovered_count += 1

        self.last_lsn = max_lsn
        
        if recovered_count > 0:
            print(f"[*] Recovery complete. {recovered_count} entries restored. Last LSN: {self.last_lsn}")
            # 복구가 완료된 최고 LSN으로 헤더 확정
            self.index.update_header(lsn=max_lsn)
            self.index.flush_to_disk()

        self.last_lsn = max_lsn
        if recovered_count > 0:
            print(f"[*] Recovery complete. {recovered_count} entries restored to Index & LSM.")
            self.index.update_header(lsn=max_lsn)
            self.index.flush_to_disk()

    def _resize_wal_callback(self, start=True, new_size=0):
        """인덱스 리사이즈 시 WAL에 트랜잭션 경계 기록"""
        state = WAL_STATE.RESIZE_START if start else WAL_STATE.RESIZE_END
        self.last_lsn += 1
        # 리사이즈 마커는 데이터가 없으므로 0/empty 값으로 기록
        self.wal.append(state, b"\0"*16, new_size, 0, 0, 0, self.last_lsn)

    # def _put_kv_storage(self, url, content, metadata=None):
        
    #     with self.index_lock.exclusive():
    #         if (self.index.used_count / self.index.bucket_count) > 0.7:
    #             self.index._resize(core_callback=self._resize_wal_callback)

    #     with self.index_lock.exclusive():
    #         self.last_lsn += 1
    #         u_hash = hashlib.sha256(url.encode()).digest()[:16]
            
    #         try:
    #             # 1. 스토리지 로그 기록 (데이터 본문)
    #             log_info = self.storage.append_record(url, content, metadata=metadata)
    #             s_id = int(log_info['shard_id'], 16)

    #             # 2. WAL PREPARE (인덱스 쓰기 전 의도 기록)
    #             self.wal.append(WAL_STATE.PREPARE, u_hash, log_info['offset'], 
    #                             log_info['length'], log_info['timestamp'], s_id, self.last_lsn)

    #             # 3. 인덱스 기록
    #             self.index._raw_insert(u_hash, log_info['offset'], log_info['length'], 
    #                                 log_info['timestamp'], s_id)

    #             # 5. 주기적 또는 매번 Checkpoint LSN 업데이트
    #             self.index.update_header(lsn=self.last_lsn)
                
    #             # 4. 인덱스 물리적 Flush (가장 중요)
    #             self.index.flush_to_disk()


    #             # 6. COMMIT 기록
    #             self.wal.append(WAL_STATE.COMMIT, u_hash, 0, 0, 0, 0, self.last_lsn)
                
    #             return log_info['url_hash']
    #         except Exception as e:
    #             print(f"[-] Put failed: {e}")
    #             return None

    # def put(self, url, content, metadata=None):
    #     # 1. KV 스토리지 저장 (기존 로직)
    #     url_hash = self._put_kv_storage(url, content, metadata)
        
    #     if url_hash:
    #         # 2. LSM 역색인에 키워드 추가
    #         # 여기서 url_hash는 16바이트 binary
    #         self.lsm_index.add_document(url_hash, content)

    #         if len(self.lsm_index.sstable_files) >= 5:
    #             self.lsm_index.compact()
            
    #     return url_hash

    # def put(self, url, content, metadata=None):
    #     u_hash = hashlib.sha256(url.encode()).digest()[:16]
    #     self.last_lsn += 1
    #     cur_lsn = self.last_lsn

    #     try:
    #         # [단계 1] Storage Append
    #         log_info = self.storage.append_record(url, content, metadata=metadata)
    #         shard_id_int = int(log_info['shard_id'], 16)
            
    #         # [단계 2] WAL PREPARE + fsync
    #         self.wal.append(WAL_STATE.PREPARE, u_hash, log_info['offset'], 
    #                         log_info['length'], log_info['timestamp'], shard_id_int, cur_lsn)
            
    #         # [단계 3] Index Data Write
    #         self.index._raw_insert(u_hash, log_info['offset'], log_info['length'], 
    #                                log_info['timestamp'], shard_id_int)
            
    #         # [단계 4] WAL COMMIT + fsync
    #         self.wal.append(WAL_STATE.COMMIT, u_hash, 0, 0, 0, 0, cur_lsn)

    #         # [단계 5] Index Header 업데이트 (Checkpoint) 및 물리적 저장
    #         # DynamicNexusIndex에는 flush_to_disk가 헤더와 데이터를 모두 포함합니다.
    #         self.index.update_header(lsn=cur_lsn)
    #         self.index.flush_to_disk() 

    #         # [LSM] 반영 (이제 에러 없이 여기까지 도달하여 검색이 가능해집니다)
    #         self.lsm_index.add_document(u_hash, content)
            
    #         return u_hash
    #     except Exception as e:
    #         print(f"[-] Transaction failed at LSN {cur_lsn}: {e}")
    #         return None

    def put(self, url, content, metadata=None):
        u_hash = hashlib.sha256(url.encode()).digest()[:16]
        self.last_lsn += 1
        cur_lsn = self.last_lsn

        try:
            # 1. Storage & WAL PREPARE
            log_info = self.storage.append_record(url, content, metadata=metadata)
            shard_id_int = int(log_info['shard_id'], 16)
            self.wal.append(WAL_STATE.PREPARE, u_hash, log_info['offset'], 
                            log_info['length'], log_info['timestamp'], shard_id_int, cur_lsn)
            
            # 2. Index Data Write (데이터만 쓰고 헤더는 아직!)
            self.index._raw_insert(u_hash, log_info['offset'], log_info['length'], 
                                   log_info['timestamp'], shard_id_int)

            # 3. LSM 반영
            self.lsm_index.add_document(u_hash, content)

            # 4. WAL COMMIT
            self.wal.append(WAL_STATE.COMMIT, u_hash, 0, 0, 0, 0, cur_lsn)

            # 5. [중요] 마지막에 헤더 업데이트 및 전체 플러시
            # 여기서 lsn을 업데이트해야, 다음 실행 시 복구 엔진이 여기까지 검사함
            self.index.update_header(lsn=cur_lsn)
            self.index.flush_to_disk()
            
            # (선택 사항) LSM이 너무 안 써진다면 강제 플러시 테스트
            # if cur_lsn % 100 == 0: self.lsm_index.flush()
            
            return u_hash
        except Exception as e:
            print(f"[-] Transaction failed: {e}")
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
    
    def search(self, keyword):
        """키워드 기반 통합 검색"""
        # 1. LSM에서 URL 해시들 확보
        url_hashes = self.lsm_index.search(keyword)
        
        results = []
        for u_hash in url_hashes:
            # 2. KV 인덱스에서 위치 조회
            idx_entry = self.index.lookup_by_hash(u_hash)
            if idx_entry:
                # 3. 실제 데이터 로드
                shard_id_str = f"{idx_entry['shard_id']:02x}"
                data = self.storage.read_record(shard_id_str, idx_entry['offset'])
                if data:
                    results.append(data)
        return results

    def ranking_search(self, keyword):
        """키워드 기반 통합 검색"""
        # 1. LSM에서 URL 해시들 확보
        url_hashes = self.lsm_index.ranking_search(keyword)
        
        results = []
        for u_hash in url_hashes:
            # 2. KV 인덱스에서 위치 조회
            idx_entry = self.index.lookup_by_hash(u_hash)
            if idx_entry:
                # 3. 실제 데이터 로드
                shard_id_str = f"{idx_entry['shard_id']:02x}"
                data = self.storage.read_record(shard_id_str, idx_entry['offset'])
                if data:
                    results.append(data)
        return results

    def delete(self, url):
        u_hash = hashlib.sha256(url.encode()).digest()[:16]
        self.last_lsn += 1
        cur_lsn = self.last_lsn

        try:
            # 1. WAL에 삭제 의도 기록 (상태값은 별도의 DELETE 상수가 있다면 사용)
            # 여기서는 편의상 PREPARE 단계에서 length를 0이나 특정 마커로 기록
            self.wal.append(WAL_STATE.PREPARE, u_hash, 0, 0, int(time.time()), 0, cur_lsn)

            # 2. KV 인덱스에서 제거
            success = self.index.remove(url)
            
            # 3. LSM 역색인에서 제거 (Tombstone 처리)
            # 역색인에서 해당 URL 해시를 가진 모든 키워드 연결을 끊어야 함
            # 보통 LSM에서는 "content" 자리에 None이나 특정 삭제 마커를 넣음
            self.lsm_index.add_document(u_hash, "") # 빈 문자열로 덮어씌워 검색 결과에서 제외

            # 4. WAL COMMIT 및 헤더 갱신
            self.wal.append(WAL_STATE.COMMIT, u_hash, 0, 0, 0, 0, cur_lsn)
            self.index.update_header(lsn=cur_lsn)
            self.index.flush_to_disk()

            return success
        except Exception as e:
            print(f"[-] Delete failed: {e}")
            return False

    def close(self):
        # 종료 전 메모리에 남은 LSM 인덱스를 파일로 저장
        if hasattr(self, 'lsm_index'):
            print("[*] Closing: Flushing LSM Memtable to disk...")
            self.lsm_index.flush() 

        if self.lsm_index.should_compact : self.run_compact()
        
        self.index.close()
        print("[*] System closed safely.")

    def run_compact(self):
        # 현재 인덱스에 있는 모든 유효한 해시를 가져오는 함수를 전달
        valid_hashes_getter = lambda: {e['u_hash'] for e in self.index.get_all_entries()}
        self.lsm_index.compact(active_hashes_provider=valid_hashes_getter)

if __name__ == "__main__":
    db_path = "my_local_search_db"
    core = NexusCore(base_dir="multi_lang_db")

    # 다국어 데이터 입력
    # core.put("https://ko.test", "파이썬 검색 엔진을 만들고 있습니다.")
    # core.put("https://jp.test", "Pythonの検索エンジンを作っています。")
    # core.put("https://en.test", "I am building a search engine with Python.")

    # 1. 한국어 검색
    print(core.search("파이썬 엔진")) 
    
    # 2. 일본어 검색
    print(core.search("検索エンジン"))
    
    # 3. 영어 검색
    print(core.search("Search Engine"))

    # core.close()