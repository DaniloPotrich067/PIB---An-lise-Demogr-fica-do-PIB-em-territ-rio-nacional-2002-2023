#!/usr/bin/env python3
"""
Executa scripts SQL no Neon em sequência com validação
"""
import sys
from pathlib import Path

try:
    import psycopg2
    from psycopg2 import sql
except ImportError:
    print("❌ psycopg2 não instalado. Instalando...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary", "-q"])
    import psycopg2

# ==================== CREDENCIAIS ====================
HOST = "ep-aged-shape-acp6lhnb-pooler.sa-east-1.aws.neon.tech"
PORT = 5432
DATABASE = "neondb"
USER = "neondb_owner"
PASSWORD = "npg_PRaskBT3dMm0"

# ==================== CONFIG ====================
SCRIPTS_DIR = Path(__file__).parent / "SQL"
SCRIPTS_ORDER = [
    "00_Reset_Schema.sql",
    "01_query.sql",
    "02_index.sql",
    "03_materialized_views.sql",
    "04_tests.sql"
]

def connect_db():
    """Conecta ao banco Neon"""
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USER,
            password=PASSWORD,
            sslmode="require"
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"❌ ERRO: Não conseguiu conectar ao Neon")
        print(f"   {e}")
        sys.exit(1)

def read_script(script_name: str) -> str:
    """Lê arquivo SQL"""
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Script não encontrado: {script_path}")
    return script_path.read_text("utf-8")

def execute_script(conn, script_name: str, script_content: str):
    """Executa um script SQL com tratamento de erro"""
    cursor = conn.cursor()
    try:
        print(f"\n{'='*60}")
        print(f"▶️  Executando: {script_name}")
        print(f"{'='*60}")
        
        # Executar script completo
        cursor.execute(script_content)
        conn.commit()
        
        print(f"✅ {script_name} executado com sucesso!")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"❌ ERRO em {script_name}:")
        print(f"   {str(e)[:200]}")
        
        # Para scripts críticos (00, 01, 02, 03), para a execução
        if script_name in ["00_Reset_Schema.sql", "01_query.sql", "02_index.sql", "03_materialized_views.sql"]:
            print(f"\n❌ Script crítico falhou. Abortando...")
            return False
        
        # Para testes (04), continua mesmo com erros
        if script_name == "04_tests.sql":
            print(f"⚠️  Testes tiveram erros, mas continuando...")
            return True
    
    finally:
        cursor.close()

def main():
    print("\n" + "="*60)
    print("🚀 INICIALIZANDO DATABASE — PIB Brasil")
    print("="*60)
    
    # Conectar
    print("\n🔌 Conectando ao Neon...")
    conn = connect_db()
    print(f"✅ Conectado a {DATABASE}@{HOST}")
    
    # Executar scripts em ordem
    failed = []
    for script_name in SCRIPTS_ORDER:
        script_content = read_script(script_name)
        success = execute_script(conn, script_name, script_content)
        if not success:
            failed.append(script_name)
            break  # Para na primeira falha crítica
    
    conn.close()
    
    # Resumo
    print(f"\n{'='*60}")
    if failed:
        print(f"❌ FALHA: Scripts que falharam: {', '.join(failed)}")
        sys.exit(1)
    else:
        print(f"✅ SUCESSO: Todos os scripts executados!")
        print(f"{'='*60}")
        print("\n📊 Próximos passos:")
        print("   1. Rode: python validate_neon.py")
        print("   2. Rode: streamlit run UI/app.py")
        print("   3. Agendar refresh com: psql ... -f 03a_setup_refresh_scheduler.sql")
        
if __name__ == "__main__":
    main()
