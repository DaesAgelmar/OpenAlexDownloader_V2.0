import argparse
import configparser
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime
from difflib import get_close_matches
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Set, Tuple
from urllib.parse import urlparse

import filelock
import pandas as pd
import PyPDF2
import requests
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential
from tqdm import tqdm


# --- Güvenlik ve Doğrulama ---
class SecurityValidator:
    """Güvenlik kontrolleri için yardımcı sınıf"""
    @staticmethod
    def is_safe_url(url: str) -> bool:
        """URL güvenlik kontrolü"""
        try:
            parsed = urlparse(url)
            if parsed.scheme not in ['http', 'https']:
                return False
            # Private IP kontrolü
            if any(parsed.netloc.startswith(prefix) for prefix in
                   ['127.', '192.168.', '10.', '172.', 'localhost']):
                return False
            return True
        except ValueError:
            return False

    @staticmethod
    def sanitize_filename(filename: str, max_length: int = 200) -> str:
        """Dosya adı güvenlik temizliği"""
        unsafe_chars = ['..', '/', '\\', '\x00', ':', '*', '?', '"', '<', '>', '|']
        for char in unsafe_chars:
            filename = filename.replace(char, '_')
        filename = re.sub(r'[^\w\s\-\.]', '_', filename)
        filename = filename.strip('. ')
        if len(filename) > max_length:
            name, ext = os.path.splitext(filename)
            filename = name[:max_length - len(ext)] + ext
        return filename or 'unnamed'

    @staticmethod
    def validate_issn(issn: str) -> Tuple[bool, str]:
        """ISSN format doğrulaması ve normalizasyonu"""
        issn = issn.strip().upper()
        patterns = [
            (r'^\d{4}-\d{3}[\dX]$', lambda x: x),
            (r'^\d{7}[\dX]$', lambda x: f"{x[:4]}-{x[4:]}")
        ]
        for pattern, normalizer in patterns:
            if re.match(pattern, issn):
                return True, normalizer(issn)
        return False, issn


# --- Gelişmiş Konfigürasyon ---
class Config:
    """Gelişmiş konfigürasyon yönetimi"""
    DEFAULT_CONFIG = {
        'API': {
            'email': 'kyayla@gmail.com',
            'base_url': 'https://api.openalex.org/works',
            'max_works_per_journal': '50',
            'timeout': '60',
            'rate_limit_per_second': '10',
            'concurrent_downloads': '5',
            'concurrent_api_calls': '2',
            'max_retries': '3',
            'user_agent': 'OpenAlexDownloader/2.0 (mailto:kyayla@gmail.com)'
        },
        'PATHS': {
            'base_dir': 'pdf_files',
            'log_dir': 'logs',
            'result_csv': 'indirilen_pdf_kayitlari.csv',
            'progress_dir': '.progress',
            'hash_db': '.pdf_hashes.json'
        },
        'PERFORMANCE': {
            'csv_chunk_size': '1000',
            'enable_pagination': 'true',
            'max_results_per_page': '200'
        },
        'VALIDATION': {
            'validate_pdf': 'true',
            'check_duplicates': 'true',
            'min_pdf_size_kb': '10',
            'max_pdf_size_mb': '100'
        }
    }

    def __init__(self, config_file: str = "config.ini"):
        self.config = configparser.ConfigParser()
        self.config_file = config_file
        self._load_or_create()

    def _load_or_create(self):
        if not os.path.exists(self.config_file):
            self.config.read_dict(self.DEFAULT_CONFIG)
            self.save()
            print(f"✅ {self.config_file} oluşturuldu. Lütfen ayarları güncelleyin (özellikle email).")
        else:
            self.config.read(self.config_file, encoding='utf-8')
            self._ensure_all_sections()

    def _ensure_all_sections(self):
        updated = False
        for section, options in self.DEFAULT_CONFIG.items():
            if section not in self.config:
                self.config[section] = options
                updated = True
            else:
                for key, value in options.items():
                    if key not in self.config[section]:
                        self.config[section][key] = value
                        updated = True
        if updated:
            self.save()

    def save(self):
        with open(self.config_file, 'w', encoding='utf-8') as f:
            self.config.write(f)

    def get(self, section: str, option: str, fallback: str = "") -> str:
        return self.config.get(section, option, fallback=fallback)

    def getint(self, section: str, option: str, fallback: int = 0) -> int:
        return self.config.getint(section, option, fallback=fallback)

    def getboolean(self, section: str, option: str, fallback: bool = False) -> bool:
        return self.config.getboolean(section, option, fallback=fallback)

    def getfloat(self, section: str, option: str, fallback: float = 0.0) -> float:
        return self.config.getfloat(section, option, fallback=fallback)


# --- Atomic File Operations ---
@contextmanager
def atomic_write(filepath: str, mode: str = 'w'):
    dir_name = os.path.dirname(filepath)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)
        
    with tempfile.NamedTemporaryFile(mode=mode, dir=dir_name, delete=False, encoding='utf-8') as tf:
        temp_path = tf.name
        try:
            yield tf
            tf.flush()
            os.fsync(tf.fileno())
        except Exception:
            os.remove(temp_path)
            raise
        else:
            os.replace(temp_path, filepath)


# --- Incremental CSV Writer ---
class IncrementalCSVWriter:
    def __init__(self, filepath: str, chunk_size: int):
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.buffer: List[Dict] = []
        self.lock = Lock()
        self._ensure_header()

    def _ensure_header(self):
        if not os.path.exists(self.filepath):
            pd.DataFrame(columns=[
                'issn', 'openalex_id', 'title', 'doi', 'pdf_url',
                'local_path', 'file_size_mb', 'pdf_hash', 'downloaded',
                'download_date', 'error_message'
            ]).to_csv(self.filepath, index=False, encoding='utf-8')

    def add_records(self, records: List[Dict]):
        with self.lock:
            self.buffer.extend(records)
            if len(self.buffer) >= self.chunk_size:
                self.flush()

    def flush(self):
        if not self.buffer:
            return
        with self.lock:
            if self.buffer:
                df = pd.DataFrame(self.buffer)
                df.to_csv(self.filepath, mode='a', header=False, index=False, encoding='utf-8')
                self.buffer = []

    def __del__(self):
        self.flush()


# --- Enhanced Progress Manager ---
class EnhancedProgressManager:
    def __init__(self, progress_dir: str):
        self.progress_dir = Path(progress_dir)
        self.progress_dir.mkdir(exist_ok=True)
        self.progress_file = self.progress_dir / "progress.json"
        self.lock = filelock.FileLock(str(self.progress_file) + ".lock")
        self.progress = self._load_progress()

    def _load_progress(self) -> Dict[str, Any]:
        try:
            with self.lock:
                if self.progress_file.exists():
                    with open(self.progress_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        data['completed_issns'] = set(data.get('completed_issns', []))
                        return data
        except Exception as e:
            logger.warning(f"Progress yüklenemedi: {e}")
            
        return {
            "completed_issns": set(), "failed_downloads": {}, "successful_downloads": {},
            "statistics": {"total_downloads": 0, "total_failures": 0, "total_size_mb": 0.0}
        }

    def save(self, is_checkpoint: bool = False):
        save_data = self.progress.copy()
        save_data["completed_issns"] = list(self.progress.get("completed_issns", set()))
        target_file = self.progress_file
        if is_checkpoint:
            checkpoint_name = datetime.now().strftime("%Y%m%d_%H%M%S")
            target_file = self.progress_dir / f"checkpoint_{checkpoint_name}.json"
            logger.info(f"Checkpoint oluşturuluyor: {target_file}")
            
        with self.lock:
            with atomic_write(str(target_file), 'w') as f:
                json.dump(save_data, f, indent=2)


# --- PDF Hash Manager ---
class PDFHashManager:
    def __init__(self, hash_db_path: str):
        self.hash_db_path = hash_db_path
        self.hashes: Set[str] = self._load_hashes()
        self.lock = Lock()

    def _load_hashes(self) -> Set[str]:
        if os.path.exists(self.hash_db_path):
            try:
                with open(self.hash_db_path, 'r', encoding='utf-8') as f:
                    return set(json.load(f))
            except json.JSONDecodeError:
                logger.warning("Hash DB okunamadı, yeni oluşturulacak.")
        return set()

    def save(self):
        with self.lock:
            with atomic_write(self.hash_db_path, 'w') as f:
                json.dump(list(self.hashes), f)

    def add_hash(self, pdf_hash: str) -> bool:
        with self.lock:
            if pdf_hash in self.hashes:
                return False
            self.hashes.add(pdf_hash)
            return True

    @staticmethod
    def calculate_file_hash(filepath: str, chunk_size: int = 8192) -> str:
        sha256 = hashlib.sha256()
        try:
            with open(filepath, 'rb') as f:
                while chunk := f.read(chunk_size):
                    sha256.update(chunk)
        except FileNotFoundError:
            return ""
        return sha256.hexdigest()


# --- Ana İndirici Sınıf ---
class OpenAlexPDFDownloader:
    def __init__(self, config_file: str):
        self.config = Config(config_file)
        self._setup_logging()
        self._setup_directories()
        self.progress = EnhancedProgressManager(self.config.get('PATHS', 'progress_dir'))
        self.csv_writer = IncrementalCSVWriter(
            self.config.get('PATHS', 'result_csv'),
            self.config.getint('PERFORMANCE', 'csv_chunk_size')
        )
        self.hash_manager = PDFHashManager(self.config.get('PATHS', 'hash_db'))
        self.session = self._create_session()

    def _setup_logging(self):
        global logger
        log_dir = self.config.get('PATHS', 'log_dir')
        log_file = os.path.join(log_dir, f"openalex_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            handlers=[logging.FileHandler(log_file, encoding='utf-8'), logging.StreamHandler()]
        )
        logger = logging.getLogger(__name__)

    def _setup_directories(self):
        for path_key in ['base_dir', 'log_dir', 'progress_dir']:
            os.makedirs(self.config.get('PATHS', path_key), exist_ok=True)

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        headers = {
            'User-Agent': self.config.get('API', 'user_agent'),
            'Accept': 'application/json'
        }
        session.headers.update(headers)
        return session

    def get_works_for_issn(self, issn: str, page: int) -> Tuple[List[Dict], bool]:
        """ISSN için makaleleri getir (pagination destekli)"""
        @retry(
            stop=stop_after_attempt(self.config.getint('API', 'max_retries')),
            wait=wait_exponential(multiplier=1, min=4, max=10)
        )
        def _get_with_retry():
            per_page = self.config.getint('PERFORMANCE', 'max_results_per_page')
            params = {
                "filter": f"primary_location.source.issn:L:{issn}",
                "sort": "publication_date:desc",
                "per_page": per_page,
                "page": page,
                "select": "id,display_name,doi,publication_date,open_access",
                "mailto": self.config.get('API', 'email')
            }
            response = self.session.get(
                self.config.get('API', 'base_url'),
                params=params,
                timeout=self.config.getint('API', 'timeout')
            )
            response.raise_for_status()
            data = response.json()
            meta = data.get('meta', {})
            works = data.get('results', [])
            has_more = (page * per_page) < meta.get('count', 0)
            return works, has_more
        
        try:
            return _get_with_retry()
        except RetryError as e:
            logger.error(f"API isteği denemelerden sonra başarısız oldu: {issn} (sayfa {page}): {e}")
            return [], False

    def get_all_works_for_issn(self, issn: str) -> List[Dict]:
        """Tüm sayfalardan makaleleri al"""
        all_works: List[Dict] = []
        page = 1
        max_works = self.config.getint('API', 'max_works_per_journal')
        if not self.config.getboolean('PERFORMANCE', 'enable_pagination'):
            works, _ = self.get_works_for_issn(issn, 1)
            return works[:max_works]

        with tqdm(desc=f"API Pages for {issn}", leave=False, unit="page") as pbar:
            while len(all_works) < max_works:
                works, has_more = self.get_works_for_issn(issn, page)
                if not works:
                    break
                all_works.extend(works)
                pbar.update(1)
                if not has_more:
                    break
                page += 1
        return all_works[:max_works]

    def validate_and_download_pdf(self, pdf_url: str, save_path: str) -> Dict[str, Any]:
        """PDF'i indir ve doğrula"""
        result: Dict[str, Any] = {'success': False, 'file_size_mb': 0.0, 'pdf_hash': None, 'error': "Bilinmeyen Hata"}
        if not SecurityValidator.is_safe_url(pdf_url):
            result['error'] = 'Güvensiz URL'
            return result

        try:
            with self.session.get(pdf_url, stream=True, timeout=self.config.getint('API', 'timeout'), allow_redirects=True) as r:
                r.raise_for_status()
                content_type = r.headers.get('content-type', '').lower()
                if 'application/pdf' not in content_type:
                    result['error'] = f'PDF değil: {content_type}'
                    return result

                total_size = int(r.headers.get('content-length', 0))
                min_size = self.config.getint('VALIDATION', 'min_pdf_size_kb') * 1024
                max_size = self.config.getint('VALIDATION', 'max_pdf_size_mb') * 1024 * 1024
                if not (min_size <= total_size <= max_size):
                    result['error'] = f'Boyut Sınır Dışı: {total_size/1024/1024:.2f}MB'
                    return result

                with atomic_write(save_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

                if self.config.getboolean('VALIDATION', 'validate_pdf'):
                    with open(save_path, 'rb') as f_pdf:
                        reader = PyPDF2.PdfReader(f_pdf)
                        if len(reader.pages) == 0:
                            raise PyPDF2.errors.PdfReadError("PDF sıfır sayfa içeriyor")

                if self.config.getboolean('VALIDATION', 'check_duplicates'):
                    pdf_hash = PDFHashManager.calculate_file_hash(save_path)
                    if not self.hash_manager.add_hash(pdf_hash):
                        result['error'] = 'Duplicate PDF'
                        os.unlink(save_path)
                        return result
                    result['pdf_hash'] = pdf_hash

                result['success'] = True
                result['file_size_mb'] = os.path.getsize(save_path) / (1024 * 1024)

        except PyPDF2.errors.PdfReadError as e:
            result['error'] = f"Geçersiz PDF: {e}"
        except Exception as e:
            result['error'] = f"{type(e).__name__}"
            
        if not result['success'] and os.path.exists(save_path):
            try:
                os.unlink(save_path)
            except OSError:
                pass
        return result

    def process_issn(self, issn: str, title: str):
        """Bir ISSN'i işle"""
        if issn in self.progress.progress.get('completed_issns', set()):
            logger.info(f"{issn} zaten tamamlanmış, atlanıyor.")
            return

        journal_dir = os.path.join(self.config.get('PATHS', 'base_dir'), SecurityValidator.sanitize_filename(issn))
        os.makedirs(journal_dir, exist_ok=True)

        works = self.get_all_works_for_issn(issn)
        if not works:
            logger.warning(f"{issn} ({title}) için makale bulunamadı.")
            self.progress.progress.setdefault('completed_issns', set()).add(issn)
            return

        tasks = []
        for work in works:
            work_id = (work.get('id') or "").split('/')[-1]
            if not work_id or work_id in self.progress.progress.get('successful_downloads', {}):
                continue
            
            pdf_url = (work.get('open_access') or {}).get('oa_url')
            if not pdf_url:
                continue

            doi = (work.get('doi') or "").replace('https://doi.org/', '').replace('/', '_')
            filename = SecurityValidator.sanitize_filename(f"{work_id}_{doi}.pdf" if doi else f"{work_id}.pdf")
            save_path = os.path.join(journal_dir, filename)

            tasks.append({'work': work, 'save_path': save_path, 'issn': issn, 'title': title})

        with ThreadPoolExecutor(max_workers=self.config.getint('API', 'concurrent_downloads')) as executor:
            future_to_task = {executor.submit(self._download_task, task): task for task in tasks}
            for future in as_completed(future_to_task):
                try:
                    future.result()
                except Exception as e:
                    failed_task = future_to_task[future]
                    logger.error(f"Download task for {failed_task.get('save_path')} failed with unhandled error: {e}")

        self.progress.progress.setdefault('completed_issns', set()).add(issn)

    def _download_task(self, task: Dict[str, Any]):
        """Tek bir indirme görevini yürüten ve kaydeden iç metod."""
        work = task['work']
        work_id = (work.get('id') or "").split('/')[-1]
        pdf_url = (work.get('open_access') or {}).get('oa_url')
        
        record: Dict[str, Any] = {
            'issn': task['issn'], 'openalex_id': work_id, 'title': work.get('display_name', ''),
            'doi': work.get('doi', ''), 'pdf_url': pdf_url, 'local_path': task['save_path'],
            'download_date': datetime.now().isoformat(), 'downloaded': False, 'file_size_mb': 0.0,
            'pdf_hash': None, 'error_message': 'No PDF URL'
        }
        
        if pdf_url:
            result = self.validate_and_download_pdf(pdf_url, task['save_path'])
            record.update({
                'downloaded': result['success'], 'file_size_mb': result['file_size_mb'],
                'pdf_hash': result['pdf_hash'], 'error_message': result['error']
            })
            
            stats = self.progress.progress['statistics']
            if result['success']:
                self.progress.progress.setdefault('successful_downloads', {})[work_id] = task['save_path']
                stats['total_downloads'] = stats.get('total_downloads', 0) + 1
                stats['total_size_mb'] = stats.get('total_size_mb', 0.0) + result['file_size_mb']
            else:
                self.progress.progress.setdefault('failed_downloads', {})[work_id] = {
                    'error': result['error'], 'timestamp': datetime.now().isoformat()
                }
                stats['total_failures'] = stats.get('total_failures', 0) + 1
        
        self.csv_writer.add_records([record])

    def run(self, excel_path: str, resume: bool):
        """Ana çalıştırma metodu"""
        try:
            issns_df = self.read_issns_from_excel(excel_path)
            if not resume:
                logger.info("Resume kapalı, sıfırdan başlanıyor.")
                self.progress.progress = {
                    "completed_issns": set(), "failed_downloads": {}, "successful_downloads": {},
                    "statistics": {"total_downloads": 0, "total_failures": 0, "total_size_mb": 0.0}
                }

            with ThreadPoolExecutor(max_workers=self.config.getint('API', 'concurrent_api_calls')) as executor:
                futures = {executor.submit(self.process_issn, row['issn'], row['title']): row['issn'] for _, row in issns_df.iterrows()}
                for future in tqdm(as_completed(futures), total=len(futures), desc="Dergiler işleniyor"):
                    issn = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"ISSN '{issn}' işlenirken kritik hata: {e}", exc_info=True)
            
            self._finalize()
        except KeyboardInterrupt:
            logger.info("\n⚠️ İşlem kullanıcı tarafından durduruldu. Kaydediliyor...")
            self._finalize()
        except Exception as e:
            logger.error(f"Kritik hata: {e}", exc_info=True)
            self._finalize()
            sys.exit(1)

    def _finalize(self):
        self.csv_writer.flush()
        self.progress.save()
        self.hash_manager.save()
        logger.info("Tüm veriler diske kaydedildi.")
        self._print_summary()

    def read_issns_from_excel(self, filepath: str) -> pd.DataFrame:
        """Excel'den ISSN'leri oku, doğrula ve temizle"""
        try:
            df = pd.read_excel(filepath, engine='openpyxl')
            df.columns = [str(col).lower().strip() for col in df.columns]
            
            issn_col, title_col = 'issn', 'title'
            # Fuzzy matching ile sütun bulma
            if issn_col not in df.columns:
                matches = get_close_matches(issn_col, df.columns, n=1)
                if matches:
                    issn_col = matches[0]
            if title_col not in df.columns:
                matches = get_close_matches(title_col, df.columns, n=1)
                if matches:
                    title_col = matches[0]

            if issn_col not in df.columns or title_col not in df.columns:
                raise ValueError("Gerekli 'ISSN' ve 'Title' sütunları bulunamadı.")
            
            df = df[[issn_col, title_col]].rename(columns={issn_col: 'issn', title_col: 'title'})
            df['is_valid'], df['issn'] = zip(*df['issn'].astype(str).apply(SecurityValidator.validate_issn))
            
            valid_df = df[df['is_valid']].drop('is_valid', axis=1).drop_duplicates('issn')
            logger.info(f"{len(valid_df)} geçerli ve benzersiz ISSN bulundu.")
            return valid_df
        except Exception as e:
            logger.error(f"Excel okuma hatası: {e}")
            raise

    def _print_summary(self):
        """Detaylı özet raporu"""
        stats = self.progress.progress.get('statistics', {})
        completed = len(self.progress.progress.get('completed_issns', set()))
        
        summary = f"""
╔════════════════════════════════════════════════╗
║                   İNDİRME RAPORU                 ║
╠════════════════════════════════════════════════╣
║ Tamamlanan ISSN Sayısı: {completed:<23}║
║ Başarılı İndirme: {stats.get('total_downloads', 0):<29}║
║ Başarısız İndirme: {stats.get('total_failures', 0):<28}║
║ Toplam Boyut: {stats.get('total_size_mb', 0.0):<25.1f} MB ║
╚════════════════════════════════════════════════╝"""
        logger.info(summary)


# --- CLI ---
def main_cli():
    """Komut satırı arayüzünü yönetir."""
    parser = argparse.ArgumentParser(
        description='OpenAlex PDF Downloader - Production Ready',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('excel_file', help='ISSN listesini içeren Excel dosyası')
    parser.add_argument('--config', default='config.ini', help='Konfigürasyon dosyası (varsayılan: config.ini)')
    parser.add_argument('--no-resume', action='store_true', help='İlerleme kayıtlarını yoksayarak baştan başla')
    
    args = parser.parse_args()
    
    downloader = OpenAlexPDFDownloader(args.config)
    downloader.run(args.excel_file, resume=not args.no_resume)

if __name__ == "__main__":
    main_cli()