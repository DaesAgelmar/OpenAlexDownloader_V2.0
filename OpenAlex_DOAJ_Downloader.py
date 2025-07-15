"""
OpenAlex PDF Downloader – v6 (Akıllı Limit ve Sıralama)
===========================================================
Bu sürüm, indirme limitinin mantığını ve API sorgulama sırasını
kullanıcı istekleri doğrultusunda günceller.

+ **Akıllı İndirme Limiti:** `max_works_per_journal` limiti artık API'den
  dönen sonuç sayısını değil, başarıyla indirilmiş ve doğrulanmış
  PDF sayısını temel alır.
+ **En Güncel Sıralaması:** API'den makaleler artık en yeniden en eskiye
  doğru (`publication_date:desc`) sıralanarak istenir.
+ **Modüler Mantık:** Limit ve başarı sayımı, doğru katman olan
  `process_issn` metoduna taşındı.
"""

# ─────────────── IMPORTS ───────────────
from __future__ import annotations
import argparse, configparser, json, logging, os, re, sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse
import pandas as pd
import requests
from tenacity import RetryError, retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from tqdm import tqdm

# ───────────────────────────────────────
#  SECURITY HELPERS & CONFIG & HELPERS (Değişiklik Yok)
# ───────────────────────────────────────
class SecurityValidator:
    # ... (Bu sınıfta değişiklik yok, aynı kalabilir)
    @staticmethod
    def is_safe_url(url: Optional[str]) -> bool:
        if not isinstance(url, str): return False
        try:
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"} or (parsed.hostname or "") in {"localhost", "127.0.0.1"}: return False
        except ValueError: return False
        return True
    @staticmethod
    def sanitize_filename(name: str, max_len: int = 200) -> str:
        reserved = {"CON", "PRN", "AUX", "NUL"}.union({f"COM{i}" for i in range(1, 10)}, {f"LPT{i}" for i in range(1, 10)})
        stem, ext = os.path.splitext(name.replace("..", "_"))
        if stem.upper() in reserved: stem = f"_{stem}"
        safe = re.sub(r"[\\/:*?\"<>|]", "_", stem).strip(" .")[: max_len - len(ext)]
        return (safe or "unnamed") + ext

class Config:
    # ... (Bu sınıfta değişiklik yok, aynı kalabilir)
    DEFAULT: Dict[str, Dict[str, str]] = {
        "API": { "email": "your_mail@example.com", "base_url": "https://api.openalex.org", "max_works_per_journal": "50", "timeout": "60", "user_agent": "OpenAlexDownloader/6.0"},
        "PATHS": { "base_dir": "pdf_files", "log_dir": "logs", "result_csv": "indirilen_pdf_kayitlari.csv", "progress_file": ".progress.json"},
        "PERFORMANCE": { "csv_chunk_size": "1000", "enable_pagination": "true", "max_results_per_page": "200"},
        "VALIDATION": { "validate_pdf": "true", "check_duplicates": "true", "min_pdf_size_kb": "10"}
    }
    def __init__(self, path: str = "config.ini") -> None:
        self.path = path
        self.cfg = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=('#', ';'))
        self._load()
    def _load(self) -> None:
        if Path(self.path).exists(): self.cfg.read(self.path, encoding="utf-8")
        else:
            self.cfg.read_dict(self.DEFAULT)
            self.save()
        changed = False
        for sec, opts in self.DEFAULT.items():
            if not self.cfg.has_section(sec):
                self.cfg.add_section(sec)
                changed = True
            for k, v in opts.items():
                if not self.cfg.has_option(sec, k):
                    self.cfg.set(sec, k, v)
                    changed = True
        if changed: self.save()
    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as fp: self.cfg.write(fp)
    def get(self, s: str, k: str, fb: str = "") -> str: return self.cfg.get(s, k, fallback=fb)
    def getint(self, s: str, k: str, fb: int = 0) -> int: return self.cfg.getint(s, k, fallback=fb)
    def getboolean(self, s: str, k: str, fb: bool = False) -> bool: return self.cfg.getboolean(s, k, fallback=fb)

class EnhancedProgressManager:
    # ... (Bu sınıfta değişiklik yok, aynı kalabilir)
    def __init__(self, file_path: str) -> None:
        self.path = Path(file_path)
        self.progress: Dict[str, Any] = {"completed_issns": set()}
        self._load()
    def _load(self) -> None:
        if not self.path.exists(): return
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
            if "completed_issns" in progress_data:
                progress_data["completed_issns"] = set(progress_data.get("completed_issns", []))
            self.progress = progress_data
        except (json.JSONDecodeError, TypeError) as e: logging.warning("İlerleme dosyası okunamadı veya bozuk (%s): %s.", self.path, e)
    def save(self) -> None:
        progress_to_save = self.progress.copy()
        progress_to_save["completed_issns"] = list(self.progress.get("completed_issns", set()))
        try:
            with open(self.path, 'w', encoding='utf-8') as f: json.dump(progress_to_save, f, indent=4)
        except TypeError as e: logging.error("İlerleme kaydedilirken hata oluştu: %s", e)

class IncrementalCSVWriter:
    # ... (Bu sınıfta değişiklik yok, aynı kalabilir)
    def __init__(self, csv_path: str, chunk: int = 500) -> None:
        self.path = Path(csv_path)
        self.chunk_size = chunk
        self.buffer: List[Dict[str, Any]] = []
        if not self.path.exists(): self.path.write_text("issn,work_id,doi,file_path,size_mb\n", encoding="utf-8")
    def add(self, row: Dict[str, Any]) -> None:
        self.buffer.append(row)
        if len(self.buffer) >= self.chunk_size: self.flush()
    def flush(self) -> None:
        if not self.buffer: return
        pd.DataFrame(self.buffer).to_csv(self.path, mode="a", header=False, index=False, encoding="utf-8")
        self.buffer.clear()

# ───────────────────────────────────────
#  ANA İNDİRİCİ (Mantık Değişiklikleri Uygulandı)
# ───────────────────────────────────────
class OpenAlexPDFDownloader:
    def __init__(self, cfg_file: str) -> None:
        self.cfg = Config(cfg_file)
        self._setup_logging()
        self._prepare_dirs()
        self.session = self._build_session()
        self.progress = EnhancedProgressManager(self.cfg.get("PATHS", "progress_file"))
        csv_chunk = self.cfg.getint("PERFORMANCE", "csv_chunk_size", 500)
        self.csv = IncrementalCSVWriter(self.cfg.get("PATHS", "result_csv"), chunk=csv_chunk)

    def _setup_logging(self) -> None:
        log_dir = Path(self.cfg.get("PATHS", "log_dir"))
        log_dir.mkdir(exist_ok=True)
        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
                            handlers=[logging.FileHandler(log_dir / "run.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)])

    def _prepare_dirs(self) -> None:
        Path(self.cfg.get("PATHS", "base_dir")).mkdir(exist_ok=True)

    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": self.cfg.get("API", "user_agent")})
        return s

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10),
           retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)))
    def _api_get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        base_url = self.cfg.get('API', 'base_url').rstrip('/')
        url = f"{base_url}/{endpoint.lstrip('/')}"
        params.setdefault("mailto", self.cfg.get("API", "email"))
        res = self.session.get(url, params=params, timeout=self.cfg.getint("API", "timeout"))
        res.raise_for_status()
        return res.json()

    def _get_source_id(self, issn: str) -> Optional[str]:
        logging.info("ISSN %s için Source ID alınıyor...", issn)
        try:
            data = self._api_get("sources", params={"filter": f"issn:{issn}"})
            if data.get("results"):
                source_id = data["results"][0].get("id")
                logging.info("Source ID bulundu: %s", source_id)
                return source_id
            logging.warning("ISSN %s için Source ID bulunamadı.", issn)
            return None
        except requests.HTTPError as e:
            logging.error("Source ID alınırken API hatası (ISSN: %s): %s", issn, e)
            return None

    def _iter_works(self, source_id: str) -> Iterable[Dict[str, Any]]:
        """Verilen Source ID için makaleleri en yeniden eskiye doğru çeker."""
        page = 1
        max_per_page = self.cfg.getint("PERFORMANCE", "max_results_per_page", 200)
        
        while True:
            params = {
                "filter": f"primary_location.source.id:{source_id},type:article",
                "page": page,
                "per-page": max_per_page,
                # YENİ: Sonuçları en yeni yayın tarihine göre sırala
                "sort": "publication_date:desc"
            }
            try:
                data = self._api_get("works", params=params)
                results = data.get("results", [])
                if not results:
                    logging.info("API'den daha fazla makale sonucu gelmedi.")
                    break
                
                yield from results
                page += 1
                
                if not self.cfg.getboolean("PERFORMANCE", "enable_pagination"):
                    break

            except requests.HTTPError as e:
                logging.error("Makaleler çekilirken API hatası (Source ID: %s): %s", source_id, e)
                break

    def _download_pdf(self, url: str, dest: Path) -> bool:
        try:
            with self.session.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            return True
        except requests.RequestException as e:
            logging.error("PDF indirme hatası (%s): %s", url, e)
            if dest.exists(): dest.unlink()
            return False

    def _is_pdf_valid(self, file_path: Path) -> bool:
        if not self.cfg.getboolean("VALIDATION", "validate_pdf"): return True
        min_size_kb = self.cfg.getint("VALIDATION", "min_pdf_size_kb", 10)
        try:
            if file_path.stat().st_size < min_size_kb * 1024:
                logging.warning("PDF (%s) min boyuttan (%d KB) küçük.", file_path.name, min_size_kb)
                return False
            with open(file_path, "rb") as f:
                if f.read(5) != b'%PDF-':
                    logging.warning("PDF (%s) geçersiz başlangıç baytlarına sahip.", file_path.name)
                    return False
        except IOError as e:
            logging.error("PDF okunurken hata (%s): %s", file_path.name, e)
            return False
        return True

    def _handle_work(self, issn: str, work: Dict[str, Any]) -> bool:
        """Tek bir makaleyi işler ve başarı durumunu döndürür."""
        oa_id = work.get("id")
        pdf_url = work.get("open_access", {}).get("oa_url")
        doi = work.get("doi")

        if not oa_id or not pdf_url or not SecurityValidator.is_safe_url(pdf_url):
            return False # Başarısız

        journal_dir = Path(self.cfg.get("PATHS", "base_dir")) / SecurityValidator.sanitize_filename(issn)
        fname = f"{SecurityValidator.sanitize_filename(oa_id.split('/')[-1])}.pdf"
        dest_path = journal_dir / fname

        if dest_path.exists():
            return False # Zaten var, yeni bir indirme sayılmaz

        if self._download_pdf(pdf_url, dest_path):
            if self._is_pdf_valid(dest_path):
                logging.info("Geçerli PDF indirildi ve kaydedildi: %s", dest_path.name)
                self.csv.add({"issn": issn, "work_id": oa_id, "doi": doi,
                              "file_path": str(dest_path),
                              "size_mb": round(dest_path.stat().st_size / (1024*1024), 4)})
                return True # Başarılı
            else:
                logging.warning("Geçersiz PDF siliniyor: %s", dest_path)
                dest_path.unlink()
        
        return False # Başarısız

    def process_issn(self, issn: str, _title: str) -> None:
        """Tek bir ISSN için tüm süreci yönetir, başarılı indirme limitini uygular."""
        if issn in self.progress.progress.get("completed_issns", set()):
            logging.info("ISSN %s daha önce işlendi, atlanıyor.", issn)
            return
            
        logging.info("--- ISSN işleniyor: %s ---", issn)
        source_id = self._get_source_id(issn)
        if not source_id: return

        journal_dir = Path(self.cfg.get("PATHS", "base_dir")) / SecurityValidator.sanitize_filename(issn)
        journal_dir.mkdir(exist_ok=True)

        # YENİ: Başarılı indirme sayacı ve limiti burada tanımlanır
        successful_downloads = 0
        max_downloads = self.cfg.getint("API", "max_works_per_journal", 0)

        # API'den gelen tüm makaleler üzerinde döngüye gir
        for work in self._iter_works(source_id):
            # YENİ: Döngünün başında limiti kontrol et
            if max_downloads > 0 and successful_downloads >= max_downloads:
                logging.info("Dergi başına %d geçerli PDF indirme limitine ulaşıldı.", max_downloads)
                break # Limite ulaşıldıysa bu dergi için daha fazla deneme

            # YENİ: _handle_work'ün sonucuna göre sayacı artır
            if self._handle_work(issn, work):
                successful_downloads += 1
        
        logging.info("ISSN %s için %d adet geçerli PDF indirildi.", issn, successful_downloads)
        self.progress.progress.setdefault("completed_issns", set()).add(issn)
        self.progress.save()

    @staticmethod
    def _load_issns(path: str) -> pd.DataFrame:
        df = pd.read_excel(path, dtype=str)
        df.columns = [c.lower().strip() for c in df.columns]
        if "issn" not in df.columns: raise ValueError("Excel dosyasında 'issn' sütunu bulunamadı.")
        if "title" not in df.columns: df["title"] = ""
        return df.fillna("")

    def run(self, excel: str) -> None:
        """Ana program döngüsü."""
        try:
            issn_df = self._load_issns(excel)
            pbar = tqdm(issn_df.itertuples(index=False), desc="Tüm ISSN'ler", total=len(issn_df))
            for row in pbar:
                pbar.set_description(f"İşleniyor: {row.issn}")
                self.process_issn(str(row.issn), str(row.title))
        except KeyboardInterrupt:
            logging.warning("\nProgram kullanıcı tarafından durduruldu! Çıkmadan önce veriler kaydediliyor...")
        except Exception as exc:
            logging.error("Beklenmedik bir ana hata oluştu: %s", exc, exc_info=True)
        finally:
            logging.info("Veri arabellekleri diske yazılıyor...")
            self.csv.flush()
            self.progress.save()
            logging.info("Tüm işlemler sonlandırıldı ✔")

# ───────────────────────────────────────
#  CLI
# ───────────────────────────────────────
def main() -> None:
    prs = argparse.ArgumentParser(description="OpenAlex PDF İndirici (v6 - Akıllı Limit ve Sıralama)")
    prs.add_argument("excel_file", help="ISSN listesini içeren Excel dosyası")
    prs.add_argument("--config", default="config.ini", help="Yapılandırma dosyası yolu")
    args = prs.parse_args()
    OpenAlexPDFDownloader(args.config).run(args.excel_file)

if __name__ == "__main__":
    main()