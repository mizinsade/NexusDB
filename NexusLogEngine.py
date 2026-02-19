import os
import struct
import hashlib
import json
import zstandard as zstd
import fcntl
from datetime import datetime

# [Header] Magic(4), Version(2), Type(2), MetaLen(4), BodyLen(4), Checksum(16) = 32 Bytes
HEADER_FORMAT = "<4sHHII16s"
MAGIC = b"NEXS"

class ShardedNexusLogEngine:
    def __init__(self, root_dir="nexus_storage"):
        self.root_dir = root_dir
        if not os.path.exists(self.root_dir):
            os.makedirs(self.root_dir)
        self.compressor = zstd.ZstdCompressor(level=3)

    def _get_shard_info(self, url_hash):
        """해시 접두사 기반으로 샤드 파일 경로 결정 (예: ab.nxs)"""
        shard_id = url_hash[:2]
        shard_path = os.path.join(self.root_dir, f"{shard_id}.nxs")
        return shard_id, shard_path

    def append_record(self, url, content, r_type=1, metadata=None):
        """데이터를 해당 샤드 로그 파일 끝에 추가하고 위치 정보를 반환"""
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        shard_id, shard_path = self._get_shard_info(url_hash)
        
        # 메타데이터 준비
        metadata = metadata or {}
        metadata['u'] = url # URL 저장 (검색 결과 확인용)
        metadata['t'] = int(datetime.now().timestamp())
        
        meta_bytes = json.dumps(metadata).encode()
        body_bytes = self.compressor.compress(content.encode())
        checksum = hashlib.md5(body_bytes).digest()
        
        # 헤더 생성
        header = struct.pack(HEADER_FORMAT, MAGIC, 1, r_type, 
                             len(meta_bytes), len(body_bytes), checksum)
        
        full_block = header + meta_bytes + body_bytes
        block_len = len(full_block)

        # 샤드 파일에 Append (Lock 적용)
        with open(shard_path, "ab") as f:
            try:
                fcntl.flock(f, fcntl.LOCK_EX) # 프로세스 간 동시성 제어
                offset = f.tell() # 추가될 위치 기록
                
                f.write(full_block)
                f.flush()
                os.fsync(f.fileno()) # Crash Safety: 물리 디스크 기록 보장
                
                # 인덱스 저장을 위한 정보 반환
                return {
                    "url_hash": url_hash,
                    "shard_id": shard_id,
                    "offset": offset,
                    "length": block_len,
                    "timestamp": metadata['t']
                }
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def read_record(self, shard_id, offset):
        """특정 샤드 파일의 오프셋에서 레코드 하나를 읽어옴"""
        shard_path = os.path.join(self.root_dir, f"{shard_id}.nxs")
        if not os.path.exists(shard_path):
            return None

        with open(shard_path, "rb") as f:
            f.seek(offset)
            header_data = f.read(32)
            if len(header_data) < 32: return None
            
            magic, ver, r_type, m_len, b_len, csum = struct.unpack(HEADER_FORMAT, header_data)
            
            if magic != MAGIC:
                raise ValueError("Corrupted record: Magic number mismatch")
                
            meta_bytes = f.read(m_len)
            body_compressed = f.read(b_len)
            
            # 무결성 검사
            if hashlib.md5(body_compressed).digest() != csum:
                raise ValueError("Corrupted record: Checksum mismatch")
                
            content = zstd.decompress(body_compressed).decode()
            return {
                "type": r_type,
                "metadata": json.loads(meta_bytes.decode()),
                "content": content
            }