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
def run_prediction(live_df):
    model_path = '/home/ramondemelo/Documents/Electricity Analysis/octopus_model.pkl'
    model = joblib.load(model_path)
    
    # Standard Features
    live_df['const'] = 1.0
    live_df['hour'] = datetime.now().hour
    live_df['workday'] = 1 if datetime.now().weekday() < 5 else 0
    
    # --- SCARCITY ADJUSTMENT ---
    # If demand is high (>30GW), we know OLS will under-predict.
    # We can flag this for future model training.
    live_df['is_scarcity'] = 1 if live_df['net_demand'].iloc[0] > 30000 else 0

    # Ensure we use the actual wind from the weather dataframe if available
    # instead of the 0 placeholder used in the main block
    if 'wind_speed_10m' not in live_df or live_df['wind_speed_10m'].iloc[0] == 0:
        # Note: In the next step, we should fetch actual wind to make this work
        pass 

    feature_list = [
        "const", "ramp_rate_mw", "lag_1", "workday", "z_radiation", "z_demand", 
        "net_demand", "shortwave_radiation", "temperature_2m", 
        "EMBEDDED_SOLAR_GENERATION", "EMBEDDED_WIND_GENERATION", 
        "hour", "wind_speed_10m", "NSL_FLOW", "PUMP_STORAGE_PUMPING", "index"
    ]
    
    X_live = live_df[feature_list]
    prediction = model.predict(X_live)
    
    # Logic Check: If demand is extreme but prediction is low, 
    # it confirms the OLS 'Mean Reversion' bias.
    return prediction[0]

# 4. MAIN EXECUTION
if __name__ == "__main__":
    args = sys.argv[1:]
    print(f"DEBUG: Received Arguments: {args}")
    
    is_jupyter = any(a.startswith('--f=') for a in args)
    is_grid = any("grid" in a.lower() for a in args) or is_jupyter or len(args) == 0
    is_weather = any("weather" in a.lower() for a in args) or is_jupyter or len(args) == 0

    print(f"🚀 Starting Live Collection...")
    
    try:
        # Fetching
        df_neso = fetch_neso_live("8a4a771c-3929-4e56-93ad-cdf13219dea5")
        df_weather = fetch_weather_live()
        df_octopus = fetch_octopus_live()

        # Combine
        latest_neso = df_neso.iloc[[0]].reset_index(drop=True)
        latest_weather = df_weather.iloc[[0]].reset_index(drop=True)
        live_df = pd.concat([latest_neso, latest_weather], axis=1)

        # Features
        current_demand = df_neso.iloc[0]['ND']
        previous_demand = df_neso.iloc[1]['ND'] 
        live_df['net_demand'] = current_demand
        live_df['lag_1'] = previous_demand
        live_df['ramp_rate_mw'] = current_demand - previous_demand
        
        MEAN_D, STD_D = 22079.675780581085, 7738.213573528129
        live_df['z_demand'] = (live_df['net_demand'] - MEAN_D) / STD_D
        live_df['z_radiation'] = (live_df['shortwave_radiation'] - 100) / 50 
        live_df['index'] = 1 

        for col in ["NSL_FLOW", "PUMP_STORAGE_PUMPING", "wind_speed_10m"]:
            live_df[col] = 0 

        print(f"📊 Demand: {current_demand}MW | Ramp: {live_df['ramp_rate_mw'].iloc[0]}MW")

        # Prediction logic
        if is_grid:
            print("🧠 Running Model Prediction...")
            try:
                predicted_price = run_prediction(live_df)
                prediction_p_kwh = predicted_price / 10
                print(f"🔮 Predicted: {prediction_p_kwh:.2f}p/kWh (£{predicted_price:.2f}/MWh)")
            except Exception as e:
                print(f"❌ Prediction failed: {e}")

        # Database logic
        print("📦 Saving to Database...")
        if is_grid:
            df_octopus.to_sql('octopus_prices_live', engine, if_exists='append', index=False)
            df_neso.to_sql('neso_demand_live', engine, if_exists='append', index=False)
        if is_weather:
            df_weather.to_sql('weather_live', engine, if_exists='append', index=False)
        
        print("✅ Success! Database updated.")

    except Exception as e:
        print(f"❌ Script failed: {e}")