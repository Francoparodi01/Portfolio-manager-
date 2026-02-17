from pathlib import Path
from typing import Dict
import json
import logging

try:
    import pandas as pd
except Exception:
    pd = None

logger = logging.getLogger(__name__)


class ExcelExporter:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def export_weekly_report(self, report: Dict) -> str:
        """Exporta el `report` a un archivo XLSX. Devuelve la ruta escrita.

        Si falla la escritura XLSX, escribe un JSON de fallback.
        """
        timestamp = report.get('metadata', {}).get('fecha_generacion', '')
        safe_ts = timestamp.replace(':', '').replace('-', '').replace('.', '') if timestamp else ''
        xlsx_path = self.output_dir / f'reporte_semana_{safe_ts}.xlsx'

        # Si pandas/no engine disponible -> fallback JSON
        if pd is None:
            logger.warning('pandas no disponible, escribiendo JSON de fallback')
            fallback = self.output_dir / f'reporte_semana_{safe_ts}.json'
            with open(fallback, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            return str(fallback)

        try:
            with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
                # Metadata
                meta = report.get('metadata', {})
                meta_df = pd.DataFrame([meta])
                meta_df.to_excel(writer, sheet_name='metadata', index=False)

                # Resumen ejecutivo (puntos clave)
                puntos = report.get('resumen_ejecutivo', {}).get('puntos_clave', [])
                resumen_df = pd.DataFrame(puntos)
                resumen_df.to_excel(writer, sheet_name='resumen_ejecutivo', index=False)

                # Métricas de riesgo
                riesgo = report.get('metricas_riesgo', {})
                riesgo_df = pd.DataFrame([riesgo])
                riesgo_df.to_excel(writer, sheet_name='metricas_riesgo', index=False)

                # Métricas de performance
                perf = report.get('metricas_performance', {})
                perf_df = pd.DataFrame([perf])
                perf_df.to_excel(writer, sheet_name='metricas_performance', index=False)

                # Concentración / top holdings
                conc = report.get('concentracion', {})
                try:
                    conc_df = pd.DataFrame(conc.get('top_holdings', []))
                except Exception:
                    conc_df = pd.DataFrame()
                conc_df.to_excel(writer, sheet_name='concentracion', index=False)

                # Proyecciones -> escribir JSON resumido en hoja
                proy = report.get('proyecciones', {})
                proy_json = json.dumps(proy, ensure_ascii=False)
                proy_df = pd.DataFrame([{'proyecciones': proy_json}])
                proy_df.to_excel(writer, sheet_name='proyecciones', index=False)

            logger.info('Excel exportado a %s', xlsx_path)
            return str(xlsx_path)
        except Exception:
            logger.exception('Fallo al escribir XLSX, escribiendo JSON como fallback')
            fallback = self.output_dir / f'reporte_semana_{safe_ts}.json'
            with open(fallback, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            return str(fallback)
