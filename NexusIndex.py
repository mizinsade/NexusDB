#NexusIndex.py

import mmap
import os
import struct
import hashlib
import shutil

# [Header] Magic(4), Ver(2), Buckets(I), Used(I), LSN(Q), IsClean(B) = 23 Bytes
# 정렬(Alignment)을 위해 24바이트로 맞추는 것이 좋습니다.
IDX_HEADER_FORMAT = "<4sHIIQ B 1x"
IDX_MAGIC = b"NIDX"
HEADER_SIZE = struct.calcsize(IDX_HEADER_FORMAT)


# [Entry] 16s(Hash), Q(Offset), I(Length), I(TS), H(ShardID), 2x(Padding) = 36 Bytes
IDX_ENTRY_STRUCT = "<16sQIIH2x"
IDX_ENTRY_SIZE = struct.calcsize(IDX_ENTRY_STRUCT)

class DynamicNexusIndex:
    def __init__(self, index_path="nexus.idx", initial_buckets=1_000_000):
        self.index_path = index_path
        self.last_lsn = 0
        self.bucket_count = 0
        self.used_count = 0
        self.is_rebuild_required = False
        self._load_or_create(initial_buckets)

    def _load_or_create(self, bucket_count):
        exists = os.path.exists(self.index_path)
        
        if not exists:
            self.f = open(self.index_path, "wb+")
            # 초기 생성 시 IsClean = 1 (깨끗함)
            header = struct.pack(IDX_HEADER_FORMAT, IDX_MAGIC, 1, bucket_count, 0, 0, 1)
            self.f.write(header)
            total_size = HEADER_SIZE + (bucket_count * IDX_ENTRY_SIZE)
            self.f.truncate(total_size)
            self.f.flush()
            os.fsync(self.f.fileno())
        else:
            self.f = open(self.index_path, "r+b")

        # 헤더 정보 로드
        self.f.seek(0)
        header_data = self.f.read(HEADER_SIZE)
        magic, ver, self.bucket_count, self.used_count, self.last_lsn, is_clean = struct.unpack(
            IDX_HEADER_FORMAT, header_data
        )

        # 비정상 종료 감지: 파일이 존재하는데 is_clean이 0이면 리빌드 필요
        if is_clean == 0 and self.last_lsn != 0:
            self.is_rebuild_required = True

        # mmap 매핑
        file_size = HEADER_SIZE + (self.bucket_count * IDX_ENTRY_SIZE)
        self.mm = mmap.mmap(self.f.fileno(), file_size)

        # 열자마자 Dirty Flag를 0으로 설정 (작업 중임을 표시)
        self._set_dirty_flag(0)

    def _set_dirty_flag(self, value):
        """IsClean 바이트(위치 22)를 직접 수정"""
        # IDX_HEADER_FORMAT 상 IsClean은 4+2+4+4+8 = 22번째 바이트
        self.mm[22:23] = struct.pack("B", value)

    def update_header(self, lsn=None, is_clean=None):
        if lsn is not None: self.last_lsn = lsn
        # # is_clean이 인자로 오면 업데이트, 아니면 기존 dirty(0) 유지
        # clean_val = is_clean if is_clean is not None else 0 
        header = struct.pack(IDX_HEADER_FORMAT, IDX_MAGIC, 1, 
                            self.bucket_count, self.used_count, self.last_lsn, 0)
        self.mm[:HEADER_SIZE] = header

    def get_last_lsn(self):
        _, _, _, _, lsn, _ = struct.unpack(IDX_HEADER_FORMAT, self.mm[:HEADER_SIZE])
        return lsn

    def flush_to_disk(self):
        """데이터를 물리 디스크로 강제 동기화 ( Durability )"""
        if hasattr(self, 'mm'):
            self.mm.flush()
            os.fsync(self.f.fileno())

    def _resize(self, core_callback):
        """Atomic Resize 구현"""
        new_count = self.bucket_count * 2
        new_path = self.index_path + ".tmp"
        
        # 1. WAL에 Resize 시작 기록 (Core 콜백)
        core_callback(start=True, new_size=new_count)
        
        # 2. 새 임시 파일 생성 및 초기화
        with open(new_path, "wb") as f:
            header = struct.pack(IDX_HEADER_FORMAT, IDX_MAGIC, 1, new_count, self.used_count, self.last_lsn)
            f.write(header)
            f.write(b"\0" * (new_count * IDX_ENTRY_SIZE))
            f.flush()
            os.fsync(f.fileno())

        # 3. 새 파일 mmap 후 데이터 재배치
        with open(new_path, "r+b") as nf:
            nmm = mmap.mmap(nf.fileno(), 0)
            for i in range(self.bucket_count):
                old_pos = HEADER_SIZE + (i * IDX_ENTRY_SIZE)
                entry = self.mm[old_pos : old_pos + IDX_ENTRY_SIZE]
                if entry[:16] != b"\0" * 16:
                    val_for_hash = struct.unpack("<Q", entry[:8])[0]
                    start_idx = val_for_hash % new_count
                    for j in range(new_count):
                        idx = (start_idx + j) % new_count
                        pos = HEADER_SIZE + (idx * IDX_ENTRY_SIZE)
                        if nmm[pos : pos + 16] == b"\0" * 16:
                            nmm[pos : pos + IDX_ENTRY_SIZE] = entry
                            break
            nmm.flush()
            os.fsync(nf.fileno())
            nmm.close()

        # 4. 기존 파일 교체 (Atomic Switch)
        self.mm.close()
        self.f.close()
        os.replace(new_path, self.index_path)
        
        # 5. 새 파일로 재로드 및 종료 기록
        self._load_or_create(new_count)
        core_callback(start=False)

    def insert(self, url, offset, length, timestamp, shard_id):
        url_hash = hashlib.sha256(url.encode()).digest()[:16]
        return self._raw_insert(url_hash, offset, length, timestamp, shard_id)

    def _raw_insert(self, url_hash, offset, length, timestamp, shard_id):
        # used_count가 정의되지 않은 에러 방지를 위해 self 호출
        val_for_hash = struct.unpack("<Q", url_hash[:8])[0]
        start_idx = val_for_hash % self.bucket_count
        
        for i in range(self.bucket_count):
            idx = (start_idx + i) % self.bucket_count
            pos = HEADER_SIZE + (idx * IDX_ENTRY_SIZE)
            
            existing_hash = self.mm[pos : pos + 16]
            if existing_hash == b"\0" * 16 or existing_hash == url_hash:
                is_new = (existing_hash == b"\0" * 16)
                new_entry = struct.pack(IDX_ENTRY_STRUCT, url_hash, offset, length, timestamp, shard_id)
                self.mm[pos : pos + IDX_ENTRY_SIZE] = new_entry
                
                if is_new:
                    self.used_count += 1
                    # 메모리 맵 헤더의 UsedCount 영역 갱신
                    self.mm[10:14] = struct.pack("<I", self.used_count)
                return True
        return False

    def lookup(self, url):
        url_hash = hashlib.sha256(url.encode()).digest()[:16]
        val_for_hash = struct.unpack("<Q", url_hash[:8])[0]
        start_idx = val_for_hash % self.bucket_count
        
        for i in range(self.bucket_count):
            idx = (start_idx + i) % self.bucket_count
            pos = HEADER_SIZE + (idx * IDX_ENTRY_SIZE)
            
            existing_hash = self.mm[pos : pos + 16]
            if existing_hash == b"\0" * 16: return None
            if existing_hash == url_hash:
                _, offset, length, ts, shard_id = struct.unpack(IDX_ENTRY_STRUCT, self.mm[pos:pos+IDX_ENTRY_SIZE])
                return {"offset": offset, "length": length, "timestamp": ts, "shard_id": shard_id}
        return None

    def lookup_by_hash(self, url_hash):
        """URL 문자열이 아닌 16바이트 해시값으로 직접 위치 조회"""
        val_for_hash = struct.unpack("<Q", url_hash[:8])[0]
        start_idx = val_for_hash % self.bucket_count
        
        for i in range(self.bucket_count):
            idx = (start_idx + i) % self.bucket_count
            pos = HEADER_SIZE + (idx * IDX_ENTRY_SIZE)
            
            existing_hash = self.mm[pos : pos + 16]
            if existing_hash == b"\0" * 16: 
                return None
            if existing_hash == url_hash:
                _, offset, length, ts, shard_id = struct.unpack(IDX_ENTRY_STRUCT, self.mm[pos:pos+IDX_ENTRY_SIZE])
                return {"offset": offset, "length": length, "timestamp": ts, "shard_id": shard_id}
        return None

    def remove(self, url):
        url_hash = hashlib.sha256(url.encode()).digest()[:16]
        val_for_hash = struct.unpack("<Q", url_hash[:8])[0]
        start_idx = val_for_hash % self.bucket_count
        
        for i in range(self.bucket_count):
            idx = (start_idx + i) % self.bucket_count
            pos = HEADER_SIZE + (idx * IDX_ENTRY_SIZE)
            
            existing_hash = self.mm[pos : pos + 16]
            if existing_hash == b"\0" * 16: 
                return False # 애초에 없음
                
            if existing_hash == url_hash:
                # 삭제 표시: 해시를 비우고 내용을 초기화
                self.mm[pos : pos + IDX_ENTRY_SIZE] = b"\0" * IDX_ENTRY_SIZE
                self.used_count -= 1
                # 헤더의 UsedCount 갱신
                self.mm[10:14] = struct.pack("<I", self.used_count)
                return True
        return False

    # NexusIndex.py 내부 수정 권고
    def get_all_entries(self):
        entries = []
        # HEADER_SIZE (22)를 사용해야 함
        self.mm.seek(HEADER_SIZE) 
        for i in range(self.bucket_count):
            data = self.mm.read(IDX_ENTRY_SIZE)
            if not data: break
            
            u_hash = data[:16]
            if any(u_hash):
                # 패딩(2x)을 포함한 언패킹
                res = struct.unpack(IDX_ENTRY_STRUCT, data)
                entries.append({
                    'u_hash': res[0], 'offset': res[1], 
                    'length': res[2], 'ts': res[3], 'shard_id': res[4]
                })
        return entries

    def rebuild_from_storage(self, storage_manager):
        """
        storage_manager는 실제 데이터 파일(Shard)을 순회하며 
        (hash, offset, length, ts, shard_id)를 yield하는 iterator를 가져야 합니다.
        """
        print("비정상 종료 감지: 인덱스 재구성을 시작합니다...")
        
        # 1. 기존 연결 정리
        self.close()
        
        # 2. 새 인덱스 파일 초기화 (기존 크기 혹은 적절한 크기로)
        recovery_path = self.index_path + ".recovery"
        new_idx = DynamicNexusIndex(recovery_path, initial_buckets=self.bucket_count)
        
        # 3. 데이터 원본으로부터 모든 엔트리 다시 삽입
        # storage_manager.get_all_records()는 실제 shard 파일을 바이트 단위로 읽어 정보를 추출하는 함수
        for record in storage_manager.get_all_records():
            new_idx._raw_insert(
                record['hash'], record['offset'], 
                record['length'], record['ts'], record['shard_id']
            )
        
        # 4. 종료 및 교체
        new_idx.update_header(lsn=storage_manager.get_current_lsn(), is_clean=1)
        new_idx.flush_to_disk()
        new_idx.close()
        
        os.replace(recovery_path, self.index_path)
        
        # 5. 다시 로드
        self._load_or_create(self.bucket_count)
        self.is_rebuild_required = False
        print("인덱스 복구 완료.")

    def close(self):
        if hasattr(self, 'mm'):
            # 정상 종료 시 IsClean을 1로 설정
            self._set_dirty_flag(1)
            self.mm.flush()
            self.mm.close()
        if hasattr(self, 'f'):
            self.f.close()