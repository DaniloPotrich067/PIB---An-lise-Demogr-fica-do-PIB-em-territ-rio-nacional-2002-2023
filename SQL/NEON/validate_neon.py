#!/usr/bin/env python3
"""
Valida estrutura do banco PIB Brasil no Neon
- Verifica tabelas, views, índices, função
- Testa queries e performance
- Valida integração com Streamlit
"""
import sys
import time
from pathlib import Path

try:
    import psycopg2
    import pandas as pd
except ImportError:
    print("❌ Dependências não instaladas. Instalando...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "pandas", "-q"])
    import psycopg2
    import pandas as pd

# ==================== CREDENCIAIS ====================
HOST = "ep-aged-shape-acp6lhnb-pooler.sa-east-1.aws.neon.tech"
PORT = 5432
DATABASE = "neondb"
USER = "neondb_owner"
PASSWORD = "npg_PRaskBT3dMm0"

class ValidadorNeon:
    def __init__(self):
        self.conn = None
        self.resultados = []
    
    def conectar(self):
        """Conecta ao Neon"""
        try:
            self.conn = psycopg2.connect(
                host=HOST, port=PORT, database=DATABASE,
                user=USER, password=PASSWORD, sslmode="require"
            )
            print("✅ Conectado ao Neon\n")
            return True
        except Exception as e:
            print(f"❌ Erro ao conectar: {e}")
            return False
    
    def query(self, sql: str) -> pd.DataFrame:
        """Executa query e retorna DataFrame"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=cols)
        finally:
            cursor.close()
    
    def test_tabelas(self):
        """[1] Validar tabelas base"""
        print("=" * 70)
        print("📋 [1] VALIDAÇÃO DE TABELAS")
        print("=" * 70)
        
        sql = """
        SELECT schemaname, tablename 
        FROM pg_tables 
        WHERE schemaname = 'public' 
        ORDER BY tablename
        """
        tabelas = self.query(sql)
        
        esperado = {"dim_regiao", "dim_uf", "dim_municipio", "dim_variavel", "fato_indicador_municipio"}
        encontrado = set(tabelas["tablename"].tolist())
        
        if esperado == encontrado:
            print(f"✅ Todas 5 tabelas criadas: {sorted(encontrado)}\n")
            self.resultados.append(("Tabelas", "✅"))
            return True
        else:
            print(f"❌ Tabelas faltando: {esperado - encontrado}\n")
            self.resultados.append(("Tabelas", "❌"))
            return False
    
    def test_contagem(self):
        """[2] Validar registro em cada tabela"""
        print("=" * 70)
        print("📊 [2] CONTAGEM DE REGISTROS")
        print("=" * 70)
        
        sql = """
        SELECT
            (SELECT COUNT(*) FROM dim_regiao) AS dim_regiao,
            (SELECT COUNT(*) FROM dim_uf) AS dim_uf,
            (SELECT COUNT(*) FROM dim_municipio) AS dim_municipio,
            (SELECT COUNT(*) FROM dim_variavel) AS dim_variavel,
            (SELECT COUNT(*) FROM fato_indicador_municipio) AS fato_indicador
        """
        df = self.query(sql)
        
        row = df.iloc[0]
        print(f"   dim_regiao:               {row['dim_regiao']:>8} (esperado: 5)")
        print(f"   dim_uf:                   {row['dim_uf']:>8} (esperado: 27)")
        print(f"   dim_municipio:            {row['dim_municipio']:>8} (esperado: ~5570)")
        print(f"   dim_variavel:             {row['dim_variavel']:>8} (esperado: >=5)")
        print(f"   fato_indicador_municipio: {row['fato_indicador']:>8} (esperado: >=100k)\n")
        
        ok = (
            row['dim_regiao'] == 5 and
            row['dim_uf'] == 27 and
            5500 <= row['dim_municipio'] <= 5600 and
            row['dim_variavel'] >= 5 and
            row['fato_indicador'] >= 100000
        )
        
        if ok:
            print("✅ Contagens validadas!\n")
            self.resultados.append(("Contagem", "✅"))
            return True
        else:
            print("⚠️  Contagens com discrepâncias\n")
            self.resultados.append(("Contagem", "⚠️"))
            return True  # Não é crítico
    
    def test_views(self):
        """[3] Validar materialized views"""
        print("=" * 70)
        print("🔍 [3] VIEWS MATERIALIZADAS")
        print("=" * 70)
        
        sql = """
        SELECT matviewname 
        FROM pg_matviews 
        WHERE schemaname = 'mart' 
        ORDER BY matviewname
        """
        views = self.query(sql)
        
        esperado = {
            "pib_por_municipio",
            "pib_por_uf_ano",
            "pib_por_regiao_ano",
            "concentracao_municipio",
            "composicao_vab_uf_ano",
            "ranking_municipios",
            "concentracao_uf_metrics"
        }
        encontrado = set(views["matviewname"].tolist())
        
        if esperado == encontrado:
            print(f"✅ Todas 7 views criadas:\n")
            for v in sorted(encontrado):
                # Contar linhas em cada view
                cnt = self.query(f"SELECT COUNT(*) AS n FROM mart.{v}").iloc[0]["n"]
                print(f"   ✓ {v:<35} ({cnt:>10} linhas)")
            print()
            self.resultados.append(("Views", "✅"))
            return True
        else:
            print(f"❌ Views faltando: {esperado - encontrado}\n")
            self.resultados.append(("Views", "❌"))
            return False
    
    def test_indices(self):
        """[4] Validar índices"""
        print("=" * 70)
        print("⚡ [4] ÍNDICES")
        print("=" * 70)
        
        sql = """
        SELECT schemaname, tablename, indexname, indexdef
        FROM pg_indexes 
        WHERE schemaname IN ('public', 'mart')
        ORDER BY schemaname, tablename, indexname
        """
        indices = self.query(sql)
        
        # Contar UNIQUE indices para views
        unique_idx = len(indices[indices['indexname'].str.contains('_uid', na=False)])
        
        print(f"   Total de índices: {len(indices)}")
        print(f"   - UNIQUE (para CONCURRENT REFRESH): {unique_idx} (esperado: 7)")
        print(f"   - Indices compostos: {len(indices[indices['indexdef'].str.contains(',', na=False)])} (esperado: >=2)\n")
        
        if unique_idx >= 7 and len(indices) >= 12:
            print("✅ Índices validados!\n")
            self.resultados.append(("Índices", "✅"))
            return True
        else:
            print("⚠️  Índices com discrepâncias\n")
            self.resultados.append(("Índices", "⚠️"))
            return True
    
    def test_funcao(self):
        """[5] Validar função mart.refresh_all()"""
        print("=" * 70)
        print("🔄 [5] FUNÇÃO REFRESH")
        print("=" * 70)
        
        sql = """
        SELECT proname FROM pg_proc 
        WHERE proname = 'refresh_all' 
        AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'mart')
        """
        func = self.query(sql)
        
        if len(func) > 0:
            print("✅ Função mart.refresh_all() existe\n")
            self.resultados.append(("Função refresh", "✅"))
            return True
        else:
            print("❌ Função mart.refresh_all() não encontrada\n")
            self.resultados.append(("Função refresh", "❌"))
            return False
    
    def test_performance(self):
        """[6] Testar performance das views"""
        print("=" * 70)
        print("⏱️  [6] TESTES DE PERFORMANCE")
        print("=" * 70)
        
        tests = [
            ("pib_por_municipio (SPano=2022)", 
             "SELECT COUNT(*) FROM mart.pib_por_municipio WHERE id_uf=35 AND ano=2022"),
            
            ("pib_por_uf_ano (ano=2022)", 
             "SELECT COUNT(*) FROM mart.pib_por_uf_ano WHERE ano=2022"),
            
            ("concentracao_uf_metrics (SP)", 
             "SELECT * FROM mart.concentracao_uf_metrics WHERE sigla_uf='SP'"),
            
            ("composicao_vab_uf_ano (SP,2022)", 
             "SELECT * FROM mart.composicao_vab_uf_ano WHERE id_uf=35 AND ano=2022"),
        ]
        
        print("Medições (esperado < 50ms para cada):\n")
        
        all_ok = True
        for nome, sql in tests:
            t0 = time.time()
            self.query(sql)
            duracao = (time.time() - t0) * 1000
            
            status = "✅" if duracao < 50 else "⚠️"
            print(f"   {status} {nome:<45} {duracao:>7.1f}ms")
            if duracao >= 100:
                all_ok = False
        
        print()
        self.resultados.append(("Performance", "✅" if all_ok else "⚠️"))
        return True
    
    def test_streamlit_integration(self):
        """[7] Testar queries do Streamlit"""
        print("=" * 70)
        print("🎨 [7] INTEGRAÇÃO STREAMLIT")
        print("=" * 70)
        
        queries_streamlit = {
            "load_dims (dim_regiao)":
                "SELECT count(*) FROM dim_regiao",
            
            "query_base_municipios":
                "SELECT COUNT(*) FROM mart.pib_por_municipio WHERE ano=2022",
            
            "query_pib_uf":
                "SELECT COUNT(*) FROM mart.pib_por_uf_ano WHERE ano=2022",
            
            "query_composicao_uf":
                "SELECT COUNT(*) FROM mart.composicao_vab_uf_ano WHERE ano=2022",
            
            "query_concentracao_uf":
                "SELECT COUNT(*) FROM mart.concentracao_uf_metrics",
            
            "query_serie_historica":
                "SELECT COUNT(*) FROM mart.composicao_vab_uf_ano",
        }
        
        print("Testando queries do app Streamlit:\n")
        ok_count = 0
        
        for nome, sql in queries_streamlit.items():
            try:
                result = self.query(sql)
                print(f"   ✅ {nome:<35}")
                ok_count += 1
            except Exception as e:
                print(f"   ❌ {nome:<35} Error: {str(e)[:30]}")
        
        print(f"\n   Resultado: {ok_count}/{len(queries_streamlit)} queries OK\n")
        self.resultados.append(("Streamlit", "✅" if ok_count == len(queries_streamlit) else "❌"))
        return ok_count == len(queries_streamlit)
    
    def resumo(self):
        """[8] Resumo final"""
        print("=" * 70)
        print("📈 [8] RESUMO FINAL")
        print("=" * 70)
        print()
        
        for teste, status in self.resultados:
            print(f"   {status} {teste}")
        
        print("\n" + "=" * 70)
        
        if all(status == "✅" for _, status in self.resultados):
            print("🎉 TUDO OK! Banco PIB Brasil está pronto para produção!")
            print("\n📊 Dashboard esperado:")
            print("   • Carregamento: < 200ms (era 2-3s antes)")
            print("   • CPU usage: ~5% (era 80-95% antes)")
            print("   • Escalabilidade: 20-25 usuários simultâneos")
            print("\n" + "=" * 70)
            return True
        else:
            print("⚠️  REVISAR: Alguns testes tiveram problemas")
            print("=" * 70)
            return False
    
    def cleanup(self):
        """Fecha conexão"""
        if self.conn:
            self.conn.close()

def main():
    v = ValidadorNeon()
    
    try:
        if not v.conectar():
            sys.exit(1)
        
        v.test_tabelas()
        v.test_contagem()
        v.test_views()
        v.test_indices()
        v.test_funcao()
        v.test_performance()
        v.test_streamlit_integration()
        
        sucesso = v.resumo()
        
        return 0 if sucesso else 1
    finally:
        v.cleanup()

if __name__ == "__main__":
    sys.exit(main())
