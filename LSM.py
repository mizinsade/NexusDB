#LSM.py
import os
import struct
import time
import collections
import heapq

HEADER_FMT = "<4sI"
MAGIC = b"NSST"

class NexusSSTable:
    def __init__(self, path):
        self.path = path
        self.sparse_index = [] 
        self.bloom_filter = None
        self._build_sparse_index()

    @staticmethod
    def write_from_memtable(memtable, file_path):
        sorted_terms = sorted(memtable.items())
        with open(file_path, "wb") as f:
            f.write(struct.pack(HEADER_FMT, MAGIC, len(sorted_terms)))
            for term, hash_set in sorted_terms:
                term_bytes = term.encode('utf-8')
                hash_list = list(hash_set)
                f.write(struct.pack("<H", len(term_bytes)))
                f.write(term_bytes)
                f.write(struct.pack("<I", len(hash_list)))
                for h in hash_list:
                    f.write(h)

    def _build_metadata(self):
        if not os.path.exists(self.path): return
        
        # 실제 구현 시에는 SSTable 파일 끝에 Bloom Filter를 저장하고 읽어와야 하지만,
        # 여기서는 간단히 초기화 시 파일 전체를 한 번 스캔하며 빌드합니다.
        with open(self.path, "rb") as f:
            f.read(4) # Magic
            count = struct.unpack("<I", f.read(4))[0]
            self.bloom_filter = NexusBloomFilter(count)
            
            for _ in range(count):
                t_len = struct.unpack("<H", f.read(2))[0]
                term = f.read(t_len).decode('utf-8')
                self.bloom_filter.add(term) # 필터에 추가
                
                h_count = struct.unpack("<I", f.read(4))[0]
                f.seek(h_count * 16, os.SEEK_CUR)

    def _build_sparse_index(self, interval=128):
        if not os.path.exists(self.path): return
        with open(self.path, "rb") as f:
            f.read(8)
            count = 0
            while True:
                pos = f.tell()
                t_len_data = f.read(2)
                if not t_len_data: break
                t_len = struct.unpack("<H", t_len_data)[0]
                term = f.read(t_len).decode('utf-8')
                if count % interval == 0:
                    self.sparse_index.append((term, pos))
                h_count = struct.unpack("<I", f.read(4))[0]
                f.seek(h_count * 16, os.SEEK_CUR)
                count += 1

    def search(self, keyword):
        """Sparse Index로 범위를 좁힌 후 이진 탐색 효과를 냄"""
        if self.bloom_filter and keyword not in self.bloom_filter:
            return set()
            
        if not self.sparse_index: return set()
        
        start_offset = 8
        for i in range(len(self.sparse_index)):
            if self.sparse_index[i][0] <= keyword:
                start_offset = self.sparse_index[i][1]
            else:
                break
        
        with open(self.path, "rb") as f:
            f.seek(start_offset)
            while True:
                t_len_data = f.read(2)
                if not t_len_data: break
                t_len = struct.unpack("<H", t_len_data)[0]
                term = f.read(t_len).decode('utf-8')
                h_count = struct.unpack("<I", f.read(4))[0]
                
                if term == keyword:
                    return {f.read(16) for _ in range(h_count)}
                if term > keyword:
                    break
                f.seek(h_count * 16, os.SEEK_CUR)
        return set()

class SSTableIterator:
    def __init__(self, path):
        self.path = path
        self.f = open(path, "rb")
        self.f.read(8)
        self.current_entry = self._next()

    def _next(self):
        buf = self.f.read(2)
        if not buf: return None
        t_len = struct.unpack("<H", buf)[0]
        term = self.f.read(t_len).decode('utf-8')
        h_count = struct.unpack("<I", self.f.read(4))[0]
        hashes = [self.f.read(16) for _ in range(h_count)]
        return (term, hashes)

    def pop(self):
        res = self.current_entry
        self.current_entry = self._next()
        return res

    def close(self): self.f.close()

import re

class LSMInvertedIndex:
    def __init__(self, index_dir, memtable_limit=5000):
        self.index_dir = index_dir
        if not os.path.exists(self.index_dir): os.makedirs(self.index_dir)
        self.memtable = collections.defaultdict(set)
        self.memtable_limit = memtable_limit
        # 파일명을 정렬하여 최신 데이터 순서(또는 생성 순서)로 로드
        self.tables = []
        sst_files = sorted([f for f in os.listdir(self.index_dir) if f.endswith(".sst")])
        for f in sst_files:
            full_path = os.path.join(self.index_dir, f)
            self.tables.append(NexusSSTable(full_path))
        
        if self.tables:
            print(f"[*] LSM Loaded {len(self.tables)} SSTables.")

    def flush(self):
        if not self.memtable: return
        # 파일 중복 방지를 위해 더 정밀한 타임스탬프 사용
        filename = f"sst_{int(time.time() * 1000000)}.sst"
        path = os.path.join(self.index_dir, filename)
        
        # 정렬된 상태로 쓰기 (이진 탐색 성능 보장)
        NexusSSTable.write_from_memtable(self.memtable, path)
        
        # OS 캐시 강제 비우기
        fd = os.open(path, os.O_RDONLY)
        os.fsync(fd)
        os.close(fd)
        
        self.memtable.clear()
        self.tables.append(NexusSSTable(path))
        print(f"[*] LSM Flush Complete: {filename}")

    def should_compact(self):
        return len(self.tables) >= 5

    @property
    def sstable_files(self):
        return [os.path.basename(t.path) for t in self.tables]

    def add_to_memtable(self, url_hash, content):
        """복구(Recovery) 및 일반 추가를 위한 내부 메서드"""
        words = self._tokenize(content)
        for word in words:
            self.memtable[word].add(url_hash)
        if len(self.memtable) >= self.memtable_limit:
            self.flush()

    def add_document(self, url_hash, content):
        words = self._tokenize(content)
        for word in words:
            self.memtable[word].add(url_hash)
        if len(self.memtable) >= self.memtable_limit:
            self.flush()

    # def _tokenize(self, text):
    #     # 1. 소문자화 및 특수문자 제거
    #     # 언더바(_)를 포함한 특수문자를 공백으로 바꿈으로써 nexus_search_key 분리
    #     text = re.sub(r'[^a-z0-9\s]', ' ', text.lower())
        
    #     # 2. 불용어 및 길이 체크 (숫자는 무조건 통과)
    #     stop_words = {'a', 'an', 'the', 'is', 'are'} # 불용어 최소화
        
    #     result = set()
    #     for w in text.split():
    #         if w in stop_words:
    #             continue
    #         # 숫자인 경우: 무조건 포함
    #         # 문자인 경우: 2글자 이상만 (너무 짧은 관사 등 방어)
    #         if w.isdigit() or len(w) > 1:
    #             result.add(w)
                
    #     return result

    def _tokenize(self, text):
        """다국어 대응 N-Gram 토크나이저 (Bi-gram)"""
        if not text: return set()
        
        # 1. 소문자화 및 특수문자 제거 (한/영/일/숫자 유지)
        text = text.lower()
        # 가-힣(한글), ぁ-ん(히라가나), ァ-ヶ(가타카나), 亜-熙(한자), a-z, 0-9 제외 제거
        text = re.sub(r'[^a-z0-9가-힣ぁ-んァ-ヶ亜-熙\s]', ' ', text)
        
        words = text.split()
        tokens = set()
        
        for word in words:
            # 숫자는 통째로 인덱싱
            if word.isdigit():
                tokens.add(word)
                continue
            
            # 2글자 미만은 그냥 추가
            if len(word) < 2:
                tokens.add(word)
                continue
            
            # Bi-gram: 2글자씩 쪼개기 (한국어/일본어 조사 무력화 및 부분 검색 지원)
            for i in range(len(word) - 1):
                tokens.add(word[i:i+2])
            
            # 영어 단어나 긴 명사를 위해 원본도 보존
            if len(word) > 2:
                tokens.add(word)
        
        return tokens
    
    def remove_document_from_memtable(self, url_hash):
        """
        메모리에 있는 특정 문서의 흔적을 지움. 
        실제 완전한 삭제는 Compaction 단계에서 index와 대조하여 수행됨.
        """
        for word in list(self.memtable.keys()):
            if url_hash in self.memtable[word]:
                self.memtable[word].remove(url_hash)
                if not self.memtable[word]:
                    del self.memtable[word]

    def compact(self, active_hashes_provider=None):
        """
        active_hashes_provider: 현재 유효한(삭제되지 않은) 해시 집합을 주는 함수
        이 함수를 통해 삭제된 데이터를 물리적으로 제거함
        """
        if len(self.tables) < 2: return
        
        # 유효한 해시 셋 가져오기 (NexusCore.index에서 관리하는 해시들)
        valid_hashes = None
        if active_hashes_provider:
            valid_hashes = active_hashes_provider()

        output_name = f"compact_{int(time.time())}.sst"
        output_path = os.path.join(self.index_dir, output_name)
        
        paths = [t.path for t in self.tables]
        iters = [SSTableIterator(p) for p in paths]
        heap = []
        for i, it in enumerate(iters):
            if it.current_entry: heapq.heappush(heap, (it.current_entry[0], i))

        with open(output_path, "wb") as f:
            f.write(struct.pack(HEADER_FMT, MAGIC, 0))
            count, last_term, current_hashes = 0, None, set()
            
            while heap:
                term, i = heapq.heappop(heap)
                entry = iters[i].pop()
                
                if term == last_term:
                    current_hashes.update(entry[1])
                else:
                    if last_term:
                        # 유효한 해시만 필터링 (삭제된 문서 제거)
                        if valid_hashes is not None:
                            current_hashes &= valid_hashes
                        
                        if current_hashes: # 필터링 후에도 해시가 남은 경우만 씀
                            self._write_entry(f, last_term, current_hashes)
                            count += 1
                    
                    last_term, current_hashes = term, set(entry[1])
                
                if iters[i].current_entry: 
                    heapq.heappush(heap, (iters[i].current_entry[0], i))

            # 마지막 남은 엔트리 처리
            if last_term:
                if valid_hashes is not None:
                    current_hashes &= valid_hashes
                if current_hashes:
                    self._write_entry(f, last_term, current_hashes)
                    count += 1
                    
            f.seek(4); f.write(struct.pack("<I", count))

        # 정리
        for it in iters: it.close()
        for p in paths: 
            try: os.remove(p)
            except: pass
        self.tables = [NexusSSTable(output_path)]
        print(f"[*] Compaction finished: {output_name}")
    
    def _streaming_merge_to_file(self, target_files, output_path):
        paths = [os.path.join(self.index_dir, f) for f in target_files]
        iters = [SSTableIterator(p) for p in paths]
        heap = []
        
        for i, it in enumerate(iters):
            if it.current_entry:
                heapq.heappush(heap, (it.current_entry[0], i))

        with open(output_path, "wb") as f:
            f.write(struct.pack("<4sI", b"NSST", 0)) # Header
            count, last_term, current_hashes = 0, None, set()
            
            while heap:
                term, i = heapq.heappop(heap)
                entry = iters[i].pop()
                if term == last_term:
                    current_hashes.update(entry[1])
                else:
                    if last_term:
                        self._write_entry(f, last_term, current_hashes)
                        count += 1
                    last_term, current_hashes = term, set(entry[1])
                
                if iters[i].current_entry:
                    heapq.heappush(heap, (iters[i].current_entry[0], i))
            
            if last_term:
                self._write_entry(f, last_term, current_hashes)
                count += 1
            
            # 카운트 업데이트
            f.seek(4); f.write(struct.pack("<I", count))
        
        for it in iters: it.close()

    def _write_entry(self, f, term, hashes):
        tb = term.encode('utf-8')
        f.write(struct.pack(f"<H{len(tb)}sI", len(tb), tb, len(hashes)))
        for h in hashes: f.write(h)

    # LSMInvertedIndex 클래스 내부
    def search(self, keyword):
        query_tokens = self._tokenize(keyword)
        results = set()
        for token in query_tokens:
            # 1. 메모리(Memtable) 검색
            results.update(self.memtable.get(token, []))
            # results = set()
            
            # 2. 디스크(SSTables) 검색
            for sst in self.tables:
                # sst.lookup -> sst.search로 변경
                disk_results = sst.search(token) 
                if disk_results:
                    results.update(disk_results)
                    # pass
        
        return list(results)

    def ranking_search(self, keyword):
        """랭킹(Scoring)이 포함된 검색"""
        query_tokens = self._tokenize(keyword)
        if not query_tokens: return []

        # hash -> score 매핑 (많이 매칭될수록 높은 점수)
        score_map = collections.Counter()
        
        for token in query_tokens:
            # 1. 메모리 검색
            if token in self.memtable:
                for h in self.memtable[token]:
                    score_map[h] += 1
            
            # 2. SSTables 검색
            for sst in self.tables:
                found_hashes = sst.search(token)
                for h in found_hashes:
                    score_map[h] += 1
        
        # 점수가 높은 순으로 정렬하여 리턴 (구글의 가장 기초적인 원리)
        # [(hash, score), ...] -> [hash, ...]
        sorted_results = [item[0] for item in score_map.most_common()]
        return sorted_results

import math
import array
import hashlib

class NexusBloomFilter:
    def __init__(self, items_count, fp_prob=0.01):
        # fp_prob: 0.01 (1% 오탐률)
        self.size = self._get_size(items_count, fp_prob)
        self.hash_count = self._get_hash_count(self.size, items_count)
        self.bit_array = array.array('B', [0] * (self.size // 8 + 1))

    def add(self, item):
        for i in range(self.hash_count):
            digest = hashlib.sha256(f"{item}{i}".encode()).digest()
            idx = int.from_bytes(digest, 'big') % self.size
            self.bit_array[idx // 8] |= (1 << (idx % 8))

    def __contains__(self, item):
        for i in range(self.hash_count):
            digest = hashlib.sha256(f"{item}{i}".encode()).digest()
            idx = int.from_bytes(digest, 'big') % self.size
            if not (self.bit_array[idx // 8] & (1 << (idx % 8))):
                return False
        return True

    def _get_size(self, n, p):
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return int(m)

    def _get_hash_count(self, m, n):
        k = (m / n) * math.log(2)
        return int(k)