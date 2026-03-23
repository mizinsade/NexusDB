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
import json
import pickle
import numpy as np
from sentence_transformers import SentenceTransformer

class LSMInvertedIndex:
    def __init__(self, index_dir, memtable_limit=5000):
        self.index_dir = index_dir
        if not os.path.exists(self.index_dir): os.makedirs(self.index_dir)
        self.memtable = collections.defaultdict(set)
        self.memtable_limit = memtable_limit
        # 파일명을 정렬하여 최신 데이터 순서(또는 생성 순서)로 로드
        self.tables = []

        # # 1. 벡터 데이터를 저장할 경로 및 변수 초기화
        # self.vector_store = {}
        # self.vector_file = os.path.join(self.index_dir, "nexus_vectors.pkl")
        # self.status_file = os.path.join(self.index_dir, "vector_status.json")
        # self.vector_store = self._load_vectors()
        # self.processed_hashes = self._load_status()

        # if self.vector_store is None:
        #     self.vector_store = {}

        # self.vector_buffer = []  # (url_hash, content) 임시 보관
        # self.vector_batch_size = 32 # 시스템 환경에 맞게 16~64 사이 조절
        
        # # 2. 임베딩 모델 로드 (다국어 및 한국어 성능이 좋은 경량 모델)
        # print("[*] Loading Embedding Model (KR-SBERT)...")
        # self.model = SentenceTransformer('snunlp/KR-SBERT-V40K-klueNLI-augSTS')

        sst_files = sorted([f for f in os.listdir(self.index_dir) if f.endswith(".sst")])
        for f in sst_files:
            full_path = os.path.join(self.index_dir, f)
            self.tables.append(NexusSSTable(full_path))
        
        if self.tables:
            print(f"[*] LSM Loaded {len(self.tables)} SSTables.")

    def flush(self):
        if not self.memtable: return

        # 플러시할 때 남은 벡터 버퍼도 전부 처리
        # self._process_vector_batch()

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

        # 디스크 저장은 이 시점에 단 한 번만!
        # self._save_vectors()
        print(f"[*] LSM Flush Complete: {filename}")

    def should_compact(self):
        return len(self.tables) >= 10

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

    # def _load_status(self):
    #     if os.path.exists(self.status_file):
    #         with open(self.status_file, 'r') as f:
    #             return set(json.load(f))
    #     return set()

    # def _save_status(self):
    #     with open(self.status_file, 'w') as f:
    #         json.dump(list(self.processed_hashes), f)

    # def _load_vectors(self):
    #     if os.path.exists(self.vector_file):
    #         try:
    #             with open(self.vector_file, 'rb') as f:
    #                 return pickle.load(f)
    #         except: return {}
    #     return {}

    # def _save_vectors(self):
    #     with open(self.vector_file, 'wb') as f:
    #         pickle.dump(self.vector_store, f)

    def add_document(self, url_hash, content):
        words = self._tokenize(content)
        for word in words:
            self.memtable[word].add(url_hash)

        # 2. 벡터 버퍼에 임시 저장 (즉시 인코딩 X)
        # if url_hash not in self.vector_store:
        #     self.vector_buffer.append((url_hash, content))
            
        # # 3. 버퍼가 꽉 차면 한 번에 Batch 인코딩 수행
        # if len(self.vector_buffer) >= self.vector_batch_size:
        #     self._process_vector_batch()
            
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
    
    # def build_vectors(self, raw_data_provider, batch_size=64):
    #     """
    #     [신규] 사후 벡터화 작업 수행 (이어하기 지원)
    #     raw_data_provider: (url_hash, content) 튜플을 반환하는 제너레이터 또는 리스트
    #     """
    #     if self.model is None:
    #         print("[*] Loading Embedding Model...")
    #         self.model = SentenceTransformer('snunlp/KR-SBERT-V40K-klueNLI-augSTS')

    #     pending_hashes = []
    #     pending_texts = []
    #     count = 0

    #     print(f"[*] Starting Vectorization... (Already processed: {len(self.processed_hashes)})")

    #     for url_hash, content in raw_data_provider:
    #         # 이미 처리된 건너뛰기 (이어하기 핵심)
    #         if url_hash in self.processed_hashes:
    #             continue

    #         pending_hashes.append(url_hash)
    #         pending_texts.append(content)

    #         # 배치 사이즈만큼 모이면 인코딩
    #         if len(pending_hashes) >= batch_size:
    #             self._execute_batch_vectorize(pending_hashes, pending_texts)
    #             pending_hashes, pending_texts = [], []
    #             count += batch_size
                
    #             # 중간 저장 (끊겼을 때를 대비)
    #             if count % (batch_size * 10) == 0:
    #                 self._save_vectors()
    #                 self._save_status()
    #                 print(f"[*] Progress: {count} documents vectorized and saved.")

    #     # 남은 데이터 처리
    #     if pending_hashes:
    #         self._execute_batch_vectorize(pending_hashes, pending_texts)
    #         self._save_vectors()
    #         self._save_status()

    #     print(f"[*] Vectorization complete. Total new: {count}")
    
    # def _execute_batch_vectorize(self, hashes, texts):
    #     vectors = self.model.encode(texts, batch_size=len(texts), show_progress_bar=False)
    #     for i, h in enumerate(hashes):
    #         self.vector_store[h] = vectors[i]
    #         self.processed_hashes.add(h)

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
        if len(self.tables) < 2: return
        
        # 1. 유효 해시 셋 미리 확보 (루프 밖에서 한 번만 수행)
        valid_hashes = None
        if active_hashes_provider:
            valid_hashes = active_hashes_provider()

        # 한 번에 열 파일 최대 개수 (OS 제한을 고려해 안전하게 64~128 추천)
        max_chunk_size = 128

        # 테이블 리스트가 하나가 될 때까지 반복 병합
        while len(self.tables) > 1:
            # 현재 테이블 목록에서 max_chunk_size만큼만 잘라냄
            chunk = self.tables[:max_chunk_size]
            remaining = self.tables[max_chunk_size:]
            
            # 병합 결과 파일명 생성 (타임스탬프 중복 방지를 위해 마이크로초 포함)
            output_name = f"compact_{int(time.time() * 1000000)}.sst"
            output_path = os.path.join(self.index_dir, output_name)
            
            print(f"[*] Batch Compacting {len(chunk)} files -> {output_name}...")
            
            # 실제 병합 수행 (내부에서 파일 open/close가 완결됨)
            self._run_single_compact_batch(chunk, output_path, valid_hashes)
            
            # 2. 병합 완료 후 청크에 포함되었던 구형 파일들 삭제
            for t in chunk:
                try:
                    if os.path.exists(t.path):
                        os.remove(t.path)
                except Exception as e:
                    print(f"[!] Cleanup Error: {e}")

            # 3. 새로 만든 결과물 + 남은 파일들을 합쳐서 다시 루프
            # 새로 생성된 파일이 포함된 NexusSSTable 객체를 생성하여 리스트 갱신
            self.tables = [NexusSSTable(output_path)] + remaining

        print(f"[*] Full Compaction finished. Current SSTables: {len(self.tables)}")

    def _run_single_compact_batch(self, target_tables, output_path, valid_hashes):
        """기존의 머지 로직을 수행하는 내부 메서드 (파일 핸들 제어)"""
        paths = [t.path for t in target_tables]
        iters = [SSTableIterator(p) for p in paths]
        heap = []
        
        try:
            for i, it in enumerate(iters):
                if it.current_entry:
                    heapq.heappush(heap, (it.current_entry[0], i))

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
                            if valid_hashes is not None:
                                current_hashes &= valid_hashes
                            if current_hashes:
                                self._write_entry(f, last_term, current_hashes)
                                count += 1
                        last_term, current_hashes = term, set(entry[1])
                    
                    if iters[i].current_entry: 
                        heapq.heappush(heap, (iters[i].current_entry[0], i))

                if last_term:
                    if valid_hashes is not None:
                        current_hashes &= valid_hashes
                    if current_hashes:
                        self._write_entry(f, last_term, current_hashes)
                        count += 1
                        
                f.seek(4)
                f.write(struct.pack("<I", count))
                
        finally:
            # [중요] 에러가 나더라도 반드시 모든 이터레이터의 파일 핸들을 닫음
            for it in iters:
                it.close()
    
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
    
    # def semantic_search(self, keyword):
    #     """임베딩 벡터를 활용한 의미 기반 검색"""
    #     query_vector = self.model.encode(keyword)
    #     scores = []
        
    #     for url_hash, doc_vector in self.vector_store.items():
    #         # 코사인 유사도 계산
    #         norm_q = np.linalg.norm(query_vector)
    #         norm_d = np.linalg.norm(doc_vector)
    #         if norm_q == 0 or norm_d == 0: continue
                
    #         sim = np.dot(query_vector, doc_vector) / (norm_q * norm_d)
    #         scores.append((url_hash, sim))
            
    #     # 유사도가 높은 순으로 정렬하여 URL 해시만 반환
    #     scores.sort(key=lambda x: x[1], reverse=True)
    #     return [item[0] for item in scores]

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
    
    # def hybrid_search_rrf(self, keyword, k=60):
    #     """
    #     RRF (Reciprocal Rank Fusion) 기법을 이용한 하이브리드 검색
    #     키워드 검색(LSM)과 의미 기반 검색(Vector) 결과를 융합합니다.
    #     """
    #     # 1. 키워드 기반 랭킹 검색 실행
    #     # kw_results = self.ranking_search(keyword)
        
    #     # 2. 벡터 기반 의미 검색 실행
    #     sem_results = self.semantic_search(keyword)
        
    #     # 3. RRF 점수 계산을 위한 딕셔너리
    #     rrf_scores = collections.defaultdict(float)
        
    #     # 키워드 검색 결과 랭킹 반영 (1위=rank 0)
    #     # for rank, url_hash in enumerate(kw_results):
    #     #     rrf_scores[url_hash] += 1.0 / (k + rank + 1)
            
    #     # 의미 검색 결과 랭킹 반영
    #     for rank, url_hash in enumerate(sem_results):
    #         rrf_scores[url_hash] += 1.0 / (k + rank + 1)
            
    #     # 4. 합산된 RRF 점수가 높은 순서대로 최종 정렬
    #     final_results = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
        
    #     # 최종 URL 해시 리스트 반환
    #     return [item[0] for item in final_results]

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