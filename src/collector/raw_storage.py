"""
Raw Storage - Audit Trail
Persiste JSON exactamente como viene del scraper
"""
import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class RawStorage:
    """
    Almacena snapshots crudos en filesystem + DB
    
    Estructura:
    data/raw/YYYY/MM/DD/snapshot_HHMMSS_<hash>.json
    """
    
    def __init__(self, base_path: Path):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
    
    def save(
        self, 
        data: Dict, 
        source: str = 'unknown',
        metadata: Optional[Dict] = None
    ) -> str:
        """
        Guarda snapshot crudo
        
        Args:
            data: Datos crudos del scraper
            source: Origen (ej: 'cocos_scraper')
            metadata: Metadata adicional
            
        Returns:
            str: Path relativo del archivo guardado
        """
        now = datetime.now()
        
        # Calcular checksum
        data_str = json.dumps(data, sort_keys=True)
        checksum = hashlib.sha256(data_str.encode()).hexdigest()[:8]
        
        # Path estructurado por fecha
        date_path = self.base_path / str(now.year) / f"{now.month:02d}" / f"{now.day:02d}"
        date_path.mkdir(parents=True, exist_ok=True)
        
        # Filename con timestamp y checksum
        filename = f"snapshot_{now.strftime('%H%M%S')}_{checksum}.json"
        filepath = date_path / filename
        
        # Envelope con metadata
        envelope = {
            'collected_at': now.isoformat(),
            'source': source,
            'checksum': checksum,
            'metadata': metadata or {},
            'data': data
        }
        
        # Guardar
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(envelope, f, indent=2, ensure_ascii=False)
        
        relative_path = filepath.relative_to(self.base_path)
        logger.info(f"Raw snapshot guardado: {relative_path}")
        
        return str(relative_path)
    
    def load(self, relative_path: str) -> Dict:
        """
        Carga snapshot crudo
        
        Args:
            relative_path: Path relativo desde base_path
            
        Returns:
            Dict con envelope completo
        """
        filepath = self.base_path / relative_path
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def is_available(self) -> bool:
        """Verifica que el directorio sea accesible"""
        return self.base_path.exists() and self.base_path.is_dir()