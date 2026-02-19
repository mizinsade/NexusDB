import mmap
import os
import struct
import hashlib
import shutil

# [Header] Magic(4), Version(2), BucketCount(I), UsedCount(I), LastLSN(Q) = 22 Bytes
IDX_HEADER_FORMAT = "<4sHIIQ"
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
        self._load_or_create(initial_buckets)

    def _load_or_create(self, bucket_count):
        exists = os.path.exists(self.index_path)
        
        # 1. 일단 r+b로 시도, 없으면 wb+로 생성 (핸들 유지)
        if not exists:
            self.f = open(self.index_path, "wb+")
            # 초기 헤더 작성
            header = struct.pack(IDX_HEADER_FORMAT, IDX_MAGIC, 1, bucket_count, 0, 0)
            self.f.write(header)
            
            # 2. 물리적 크기 강제 할당 (이게 핵심입니다)
            total_size = HEADER_SIZE + (bucket_count * IDX_ENTRY_SIZE)
            self.f.truncate(total_size) 
            self.f.seek(0, os.SEEK_END)
            
            # 만약 truncate가 작동하지 않는 환경을 대비해 마지막 바이트에 직접 쓰기
            if self.f.tell() < total_size:
                self.f.seek(total_size - 1)
                self.f.write(b'\0')
                
            self.f.flush()
            os.fsync(self.f.fileno())
        else:
            self.f = open(self.index_path, "r+b")

        # 3. 파일 크기 재검증
        actual_size = os.path.getsize(self.index_path)
        if actual_size == 0:
            # 이 시점에도 0이라면, OS의 권한 문제이거나 디스크 용량 부족일 가능성이 큽니다.
            # 수동으로 강제 쓰기를 한 번 더 시도
            self.f.write(struct.pack(IDX_HEADER_FORMAT, IDX_MAGIC, 1, bucket_count, 0, 0))
            self.f.flush()
            os.fsync(self.f.fileno())
            actual_size = os.path.getsize(self.index_path)

        # 4. mmap 매핑 (파일 크기를 명시적으로 전달)
        # 헤더를 먼저 읽어 실제 버킷 수를 파악
        self.f.seek(0)
        header_data = self.f.read(HEADER_SIZE)
        if len(header_data) != HEADER_SIZE:
            raise ValueError("Corrupted index header")
        _, _, self.bucket_count, self.used_count, self.last_lsn = struct.unpack(
            IDX_HEADER_FORMAT, header_data
        )
        
        file_size = HEADER_SIZE + (self.bucket_count * IDX_ENTRY_SIZE)
        self.mm = mmap.mmap(self.f.fileno(), file_size)


    def update_header(self, lsn=None):
        """메모리 맵 헤더 갱신"""
        if lsn is not None: 
            self.last_lsn = lsn
        header = struct.pack(IDX_HEADER_FORMAT, IDX_MAGIC, 1, 
                             self.bucket_count, self.used_count, self.last_lsn)
        self.mm[:HEADER_SIZE] = header

    def get_last_lsn(self):
        _, _, _, _, lsn = struct.unpack(IDX_HEADER_FORMAT, self.mm[:HEADER_SIZE])
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

    def close(self):
        if hasattr(self, 'mm'): self.mm.close()
        if hasattr(self, 'f'): self.f.close()