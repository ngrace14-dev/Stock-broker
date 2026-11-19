# Stock Broker (v3.7 DEFCON 0) & Web Wrapper Terminal

A Python-based automated stock broker engine featuring a responsive web wrapper dashboard (`server.py` + `templates/index.html`), 30% systemic exposure firewall (`router.py`), ACID ledger invariant validator (`ledger_validator.py`), two-phase async position flatten (`execution.py`), and Tier-0/1 graduation backtester (`backtest_beta.py`).

## Configuring Your API Keys (`.env`)

Your credentials are kept strictly local inside `.env` (which is ignored by Git via `.gitignore`).

1. Open `c:\Users\NicholasGrace\OneDrive - RRELLC\Documents\GitHub\Stock-broker\.env` in VS Code.
2. Replace the placeholder values with your actual API key and secret:
   ```env
   ALPACA_API_KEY=your_actual_key_id
   ALPACA_SECRET_KEY=your_actual_secret_key
   ALPACA_BASE_URL=https://paper-api.alpaca.markets
   ALPACA_PAPER=true
   ```
   *(If you are also storing a Gemini API key for signal extensions, you can place it in `GEMINI_API_KEY=` in the same `.env` file.)*

> **Note:** Until real Alpaca keys are entered in `.env`, the system automatically runs in **Simulation Sandbox Mode** (`SIM-DEFCON0-001`) so you can test order routing, the 30% firewall cap, two-phase flatten, and ledger reconciliation immediately. Once your keys are saved in `.env`, click **Refresh Telemetry** in the dashboard (or restart the server) and it will automatically switch to live Alpaca Paper/Live endpoints (`/v2/account`, `/v2/positions`, `/v2/orders`).

## Running the Web Wrapper & Viewport Dashboard

Run the server using your local Python 3.14 interpreter:

```powershell
& 'C:\Users\NicholasGrace\.local\bin\python3.14.exe' server.py
```

Then open your browser to:
- **http://127.0.0.1:8000**

## Running Verification Tests

```powershell
& 'C:\Users\NicholasGrace\.local\bin\python3.14.exe' test_web_wrapper.py
```

