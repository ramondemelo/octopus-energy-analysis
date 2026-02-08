import sys
import os
import requests
import pandas as pd
import joblib
from dotenv import load_dotenv
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# 1. SETUP & ENVIRONMENT
load_dotenv()
DB_PASS = os.getenv('DB_PASSWORD')
engine = create_engine(f'postgresql://postgres:{DB_PASS}@localhost:5432/energy_warehouse')

# DEBUG INFO
print(f"DEBUG: Python Executable: {sys.executable}")

# 2. DATA FETCHING FUNCTIONS
def fetch_neso_live(resource_id):
    sql_query = f'''SELECT * FROM "{resource_id}" ORDER BY "SETTLEMENT_DATE" DESC, "SETTLEMENT_PERIOD" DESC LIMIT 48'''
    url = f"https://api.neso.energy/api/3/action/datastore_search_sql?sql={sql_query}"
    response = requests.get(url)
    data = response.json()['result']['records']
    df = pd.DataFrame(data)
    cols_to_fix = ['ND', 'EMBEDDED_SOLAR_GENERATION', 'EMBEDDED_WIND_GENERATION']
    df[cols_to_fix] = df[cols_to_fix].apply(pd.to_numeric)
    return df

def fetch_weather_live():
    url = "https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&hourly=temperature_2m,shortwave_radiation&forecast_days=1"
    r = requests.get(url)
    weather_data = r.json()['hourly']
    df = pd.DataFrame(weather_data)
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    df_30min = df.resample('30min').interpolate(method='linear')
    return df_30min.reset_index()

def fetch_octopus_live():
    url = "https://api.octopus.energy/v1/products/AGILE-24-10-01/electricity-tariffs/E-1R-AGILE-24-10-01-A/standard-unit-rates/"
    r = requests.get(url) 
    if r.status_code == 200:
        data = r.json()['results']
        df = pd.DataFrame(data)
        df['valid_from'] = pd.to_datetime(df['valid_from'])
        return df[['valid_from', 'value_inc_vat']]
    return None

# 3. PREDICTION ENGINE
def run_prediction(last_known_price, last_trend_value):
    model_path = '/home/ramondemelo/Documents/Electricity Analysis/octopus_model.pkl'
    model = joblib.load(model_path)
    
    # Define the 50 features your OLS model expects (must match your summary order)
    # Note: Replace 'model_features' with the actual list from X_train.columns
    model_features = model.params.index.tolist()
    
    # Determine the next two slots based on current time
    now = datetime.now()
    # Round to the next 30-min boundaries
    start_time = now.replace(minute=30 if now.minute < 30 else 0, second=0, microsecond=0)
    if now.minute >= 30:
        start_time += timedelta(hours=1)
        
    future_times = [start_time, start_time + timedelta(minutes=30)]
    
    current_lag = last_known_price
    results = []

    for i, t in enumerate(future_times):
        slot_str = t.strftime("%H:%M")
        
        # Build feature row
        row = pd.DataFrame(0, index=[0], columns=model_features)
        row['const'] = 1.0
        row['price_lag_1'] = current_lag
        row['trend'] = last_trend_value + (i + 1)
        
        # Activate the specific dummy slot (e.g., slot_11:30)
        slot_col = f'slot_{slot_str}'
        if slot_col in row.columns:
            row[slot_col] = 1
            
        # Predict and store
        pred = model.predict(row[model_features])[0]
        results.append((slot_str, pred))
        
        # Update lag for the second prediction
        current_lag = pred
        
    return results

# 4. MAIN EXECUTION
if __name__ == "__main__":
    args = sys.argv[1:]
    print(f"DEBUG: Received Arguments: {args}")
    
    is_jupyter = any(a.startswith('--f=') for a in args)
    is_grid = any("grid" in a.lower() for a in args) or is_jupyter or len(args) == 0
    is_weather = any("weather" in a.lower() for a in args) or is_jupyter or len(args) == 0

    print(f"🚀 Starting Live Collection...")
    
    try:
        # Fetching fresh data
        df_neso = fetch_neso_live("8a4a771c-3929-4e56-93ad-cdf13219dea5")
        df_weather = fetch_weather_live()
        df_octopus = fetch_octopus_live()

        if df_octopus is None or df_octopus.empty:
            raise ValueError("Could not fetch Octopus data. Prediction aborted.")

        # Ensure Octopus data is sorted by time (latest first)
        df_octopus = df_octopus.sort_values('valid_from', ascending=False)

        # Prediction logic
        if is_grid:
            print("🧠 Running Recursive Model Prediction...")
            try:
                # 1. Get the most recent price to use as lag_1
                last_price = df_octopus['value_inc_vat'].iloc[0]

                # 2. Get current trend (total count of historical records)
                # This ensures the trend variable matches your training scale
                with engine.connect() as conn:
                    import sqlalchemy
                    # Replace 'octopus_prices_live' with your master table name if different
                    res = conn.execute(sqlalchemy.text("SELECT count(*) FROM octopus_prices_live"))
                    current_trend = res.scalar() or 0

                # 3. Run the prediction (returns list of tuples)
                predictions = run_prediction(last_price, current_trend)
                
                print(f"--- Forecast Results (Last Actual: {last_price}p) ---")
                for slot, price in predictions:
                    print(f"🔮 Predicted for {slot}: {price:.2f}p/kWh")
                print("--------------------------------------------------")

            except Exception as e:
                print(f"❌ Prediction failed: {e}")

        # Database logic
        print("📦 Saving to Database...")
        if is_grid:
            # We append the new Octopus rates we just fetched
            df_octopus.to_sql('octopus_prices_live', engine, if_exists='append', index=False)
            df_neso.to_sql('neso_demand_live', engine, if_exists='append', index=False)
        if is_weather:
            df_weather.to_sql('weather_live', engine, if_exists='append', index=False)
        
        print("✅ Success! Database updated.")

    except Exception as e:
        print(f"❌ Script failed: {e}")