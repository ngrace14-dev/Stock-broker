import os
from pathlib import Path


def load_env_file(env_path=None):
    """Load key=value pairs from .env into os.environ without external dependencies."""
    path = Path(env_path) if env_path else Path(__file__).resolve().parent / '.env'
    if not path.exists():
        return {}

    loaded = {}
    with path.open('r', encoding='utf-8') as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, value = line.split('=', 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                os.environ.setdefault(key, value)
                loaded[key] = os.environ[key]
    return loaded


def is_real_credential(val):
    if not val:
        return False
    placeholders = {
        'YOUR_ALPACA_API_KEY_HERE',
        'YOUR_ALPACA_SECRET_KEY_HERE',
        'YOUR_GEMINI_API_KEY_HERE',
        'YOUR_PERSONAL_BETA_PASSWORD_HERE',
        'CHANGE_ME',
        'NONE',
        'NULL',
    }
    return val.strip().upper() not in placeholders


def get_broker_config():
    load_env_file()
    api_key = os.environ.get('ALPACA_API_KEY', '').strip()
    secret_key = os.environ.get('ALPACA_SECRET_KEY', '').strip()
    base_url = os.environ.get('ALPACA_BASE_URL', 'https://paper-api.alpaca.markets').strip().rstrip('/')
    paper_flag = os.environ.get('ALPACA_PAPER', 'true').strip().lower() in ('1', 'true', 'yes', 'paper')
    gemini_key = os.environ.get('GEMINI_API_KEY', '').strip()
    host = os.environ.get('HOST', '127.0.0.1').strip()
    port = int(os.environ.get('PORT', '8000').strip())
    remote_user = os.environ.get('REMOTE_AUTH_USER', 'commander').strip() or 'commander'
    remote_pass = os.environ.get('REMOTE_AUTH_PASS', '').strip()

    has_alpaca_keys = is_real_credential(api_key) and is_real_credential(secret_key)
    has_gemini_key = is_real_credential(gemini_key)
    has_remote_pass = is_real_credential(remote_pass)

    return {
        'alpaca_api_key': api_key if has_alpaca_keys else '',
        'alpaca_secret_key': secret_key if has_alpaca_keys else '',
        'alpaca_base_url': base_url,
        'alpaca_paper': paper_flag or ('paper-api' in base_url),
        'has_alpaca_keys': has_alpaca_keys,
        'gemini_api_key': gemini_key if has_gemini_key else '',
        'has_gemini_key': has_gemini_key,
        'host': host,
        'port': port,
        'remote_auth_user': remote_user,
        'remote_auth_pass': remote_pass if has_remote_pass else '',
        'has_remote_auth': has_remote_pass,
        'key_preview': (api_key[:4] + '...' + api_key[-4:]) if has_alpaca_keys and len(api_key) >= 8 else 'NOT_CONFIGURED',
    }
