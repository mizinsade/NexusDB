import struct
import os
import zlib
import fcntl

# LSN(Q), State(B), Hash(16s), Offset(Q), Len(I), TS(I), Shard(H), CRC(I) = 44 Bytes
WAL_ENTRY_FMT = "<Q B 16s Q I I H I"
WAL_SIZE = struct.calcsize(WAL_ENTRY_FMT)

class WAL_STATE:
    PREPARE = 1
    COMMIT = 2
    RESIZE_START = 3
    RESIZE_END = 4

class NexusWAL:
    def __init__(self, wal_path):
        self.wal_path = wal_path
        self.current_lsn = 0

    def _calc_crc(self, data):
        return zlib.crc32(data) & 0xFFFFFFFF

    def append(self, state, u_hash, offset, length, timestamp, shard_id, lsn):
        # CRC를 제외한 데이터 패킹
        core_data = struct.pack("<Q B 16s Q I I H", 
                                lsn, state, u_hash, offset, length, timestamp, shard_id)
        crc = self._calc_crc(core_data)
        full_entry = core_data + struct.pack("<I", crc)

        with open(self.wal_path, "ab") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(full_entry)
            f.flush()
            os.fsync(f.fileno()) # 물리적 기록 보장
            fcntl.flock(f, fcntl.LOCK_UN)

    def read_all(self):
        """복구를 위해 WAL 전체를 읽어 유효한 엔트리만 반환"""
        if not os.path.exists(self.wal_path): return []
        
        entries = []
        with open(self.wal_path, "rb") as f:
            while True:
                buf = f.read(WAL_SIZE)
                if len(buf) < WAL_SIZE: break
                
                data_part = buf[:-4]
                stored_crc = struct.unpack("<I", buf[-4:])[0]
                
                if self._calc_crc(data_part) != stored_crc:
                    break # 크래시로 인한 부분 기록(Partial Write) 발견 시 중단
                
                entries.append(struct.unpack("<Q B 16s Q I I H", data_part))
        return entries

    def checkpoint(self):
        """복구 완료 후 안전하게 WAL 초기화"""
        if os.path.exists(self.wal_path):
            with open(self.wal_path, "wb") as f:
                f.truncate(0)
                os.fsync(f.fileno())

    