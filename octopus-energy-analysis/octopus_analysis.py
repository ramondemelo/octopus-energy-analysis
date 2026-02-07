# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec

# %%
# Read Tables
neso=pd.read_csv("/home/ramondemelo/Documents/Electricity Analysis/demand_data_2025.csv")
octopus=pd.read_csv("/home/ramondemelo/Documents/Electricity Analysis/octopus_prices_full_2025.csv")
weather=pd.read_csv("/home/ramondemelo/Documents/Electricity Analysis/uk_weather_2025_historical.csv")


# %%
neso.info()

# %%
octopus.info()

# %%
weather.info()


# %%
print("-" * 65)
print("Weather data is dispensed in 1 hour granularity hence only 8760.")
print("Action: upsample to 30 minutes before performing joins.")
print("-" * 65)


# %%
print("Data Integrity Check")
print("-" * 85)
print("The yearly datasets should contain 365*48, or 17520 observations")
print("There are currently 17520 observations, except for the weather data as described above")
print("There is only one column with missing data accros all the datasets: octopus 'payment_method' ")
print("-" * 85)


# %%
neso = ((neso.assign(renewable_penetration=(neso["EMBEDDED_SOLAR_GENERATION"] + 
             neso["EMBEDDED_WIND_GENERATION"]) / neso["ND"]))
)
neso.head(2)



# %%
weather["time"] = pd.to_datetime(weather["time"])
weather.set_index("time", inplace=True)

weather = weather.resample("30min").interpolate(method="linear")
octopus.reset_index(inplace=True)
weather.reset_index(inplace=True)

master_data = pd.concat([neso, octopus, weather], axis=1).reindex(weather.index)


# %%
master_data.head(3)


# %%
master_data.info()


# %%
master_data.set_index("time", inplace=True) # set index to datetime object for further temporal exploration
master_data.head(3)



# %%
import matplotlib.pyplot as plt
fig, ax = plt.subplots()

(master_data[["ND"]]
    .resample("D")
    .mean()
    .plot(ax = ax)
)

# ax.axhline(price_weather.resample("D").mean()["octopus_price"].mean(), label= "Mean Daily Price", color = "green")
# ax.legend()


# %%
# LAG INDICATOR TO CHECK FOR AUTOCORRELATION (PREDICTABILITY)
master_data["lag_1"] = master_data["ND"].shift(1)
print("Current Period Demand Highly Correlated with Previous Period Demand")

print("-" * 70)

print(f'Correlation: {round(master_data[["ND", "lag_1"]].corr().iloc[0,1],2)}')


# %%
# PEAK EXTRACTION - LOCATING SPECIFIC HOURS OF HIGH STRESS. CHANGE FREQ TO "D" TO SPOT 
# DAILY SHIFTS IN PEAK DEMAND THROUGH THE YEAR
(master_data
    .assign(date = master_data["SETTLEMENT_DATE"].astype("datetime64[ns]"))
    .set_index("date")
    .pivot_table(values="ND", index=pd.Grouper(freq="ME"), columns="SETTLEMENT_PERIOD", aggfunc="max")
    .transpose()
    .style
    .format("{:.0f}")
    .highlight_max()
)


# %%
fig, ax= plt.subplots()
master_data["ND"].rolling(window=336).mean().plot(ax=ax)



# %%
# WORKDAY LOGIC TO QUANTIFY IMPACT OF WEEKENDS
master_data["SETTLEMENT_DATE"]=pd.to_datetime(master_data["SETTLEMENT_DATE"])
master_data["week_day"] = master_data["SETTLEMENT_DATE"].dt.dayofweek
master_data.set_index("SETTLEMENT_DATE")



# %%

# 2. SCALING: Calculate Z-Scores (Standardization) 
# This makes '0' the average, so we can see relative movement
master_data['z_demand'] = (master_data['ND'] - master_data['ND'].mean()) / master_data['ND'].std()
master_data['z_radiation'] = (master_data['shortwave_radiation'] - master_data['shortwave_radiation'].mean()) / master_data['shortwave_radiation'].std()


# 3. PLOT A: THE "SUNNY WEEK" ZOOM (Visualizing the daily relationship)
# We pick a week in July where solar impact is highest
sunny_week = master_data.loc['2025-07-01':'2025-07-07']

sunny_week["SETTLEMENT_DATE"] = pd.to_datetime(sunny_week["SETTLEMENT_DATE"])
sunny_week.set_index("SETTLEMENT_DATE", inplace=True)


plt.figure(figsize=(15, 6))
plt.plot(sunny_week.index, sunny_week['z_demand'], label='Demand (Z-Score)', color='blue', alpha=0.7)
plt.plot(sunny_week.index, sunny_week['z_radiation'], label='Radiation (Z-Score)', color='orange', alpha=0.5)

plt.title("Demand vs. Solar Radiation: A Sunny Week in July (Standardized)")
plt.ylabel("Standard Deviations from Mean")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()

# 4. PLOT B: THE "AVERAGE DAY" (The Duck Curve Evidence)
# Grouping by Settlement Period (1-48) to see the typical daily cycle
daily_profile = master_data.groupby('SETTLEMENT_PERIOD')[['z_demand', 'z_radiation']].mean()

plt.figure(figsize=(12, 6))
plt.plot(daily_profile.index, daily_profile['z_demand'], marker='o', label='Avg Demand Profile', color='blue')
plt.plot(daily_profile.index, daily_profile['z_radiation'], marker='s', label='Avg Radiation Profile', color='orange')

# Adding a visual highlight for the "Green Dent" (Midday)
plt.axvspan(20, 32, color='green', alpha=0.1, label='Solar Peak / Demand Dip')

plt.title("The 'Duck Curve' Relationship: Average Daily Profile (2025)")
plt.xlabel("Settlement Period (1 = 00:00, 48 = 23:30)")
plt.ylabel("Standardized Value")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()



# %%
sunny_week.head()


# %%
# CUMULATIVE GENERATION - TRACK YEARLY GREEN ENERGY GENERATION PROGRESS AGAINST NET ZERO TARGETS 
cumulative_monthly_wind = (master_data.pivot_table(values="EMBEDDED_WIND_GENERATION", 
index=pd.Grouper(freq="ME")).cumsum())

plt.figure(figsize=(10,5))
plt.plot(cumulative_monthly_wind.index,
        cumulative_monthly_wind["EMBEDDED_WIND_GENERATION"])


# %%


# %%
master_data["ND"].std()


# %%
master_data[["ND", "shortwave_radiation"]].corr()


# %%
master_data[["z_demand", "z_radiation"]].corr()



# %%
# IDENTIFY OCTOPUS'S PLUNGE PRICE EVENTS
master_data["price_category"] = np.where(master_data["value_inc_vat"] < 0, "plunge", "chargeable")
master_data["valid_from"] = pd.to_datetime(master_data["valid_from"])
master_data.set_index("valid_from", inplace=True)
master_data["hour"] = master_data.index.hour
master_data.head()

# %%
plunge_events = master_data[master_data["value_inc_vat"] < 0].copy()
fig, ax=plt.subplots(figsize=(10,6))
plunge_events["hour"].value_counts().sort_index().plot(kind="bar", color="blue", 
alpha=0.4, title = "Daily Distribution of Negative Prices", ylabel="Count",
xlabel="Period (Hour)", ax=ax)
ax.axvspan(8,14, color="grey", alpha=0.4)

print(f'There were {(289*30) / 60} hours of negative pricing events in 2025')
print("""Most events happened between 8:00 and 14:00. There were no instances 
of negative prices between 15:00 and 19:00 """)

# %%

# HEAT PUMP PROXY - 
bins=[0,150,450,900]
labels=["low", "medium", "strong"]
master_data["radiation_strength"] = pd.cut(master_data["shortwave_radiation"], bins=bins, labels=labels)
master_data["radiation_strength"].value_counts()


# %%



# %%
# 12. Heat Pump Proxy NESO/Weather Filtering demand when Temp $< 10$°C.
# Isolates the "Heating Load" vs. "Baseload."
cold_days = master_data[master_data["temperature_2m"] < 10].copy()
therma_corr = cold_days["temperature_2m"].corr(cold_days["ND"])

plt.figure(figsize=(10,6))
plt.scatter(master_data["temperature_2m"], master_data["ND"],
alpha=0.1,s=1, color="blue")
plt.title("Grid Sensitivity: Temperature vs. National Demand")
plt.xlabel("Temperature (°C)")
plt.ylabel("National Demand (MW)")
plt.axvline(x=15, color='red', linestyle='--', label='The Heating Threshold')
plt.legend()
plt.show()


# %%
import statsmodels.api as sm

X = sm.add_constant(master_data["temperature_2m"])
y = master_data["ND"]
model=sm.OLS(y, X).fit()
model.summary()

# %%
# heatpump proxy (workday column)
master_data.columns.tolist()
master_data["workday"] = master_data.index.dayofweek

master_data.head()
master_data.columns.tolist()

# %%
master_data["lag_1"] = master_data["lag_1"].fillna(0)
X = sm.add_constant(master_data[["temperature_2m", "workday", "lag_1"]])
y = master_data["ND"]
model=sm.OLS(y, X).fit()
model.summary()

# %%
master_data.head()

# %%
master_data["ramp_rate_mw"] = master_data["ND"].diff()
master_data["ramp_rate_mw"] = master_data["ramp_rate_mw"].fillna(0)
master_data.head()

# %%
master_data["net_demand"] = master_data["ND"] - (master_data["EMBEDDED_WIND_GENERATION"] +
master_data["EMBEDDED_SOLAR_GENERATION"])

X = master_data[["ramp_rate_mw", "lag_1", "workday", "temperature_2m", "net_demand"]]
y = master_data["value_inc_vat"]
model = sm.OLS(y, X).fit()
model.summary()


# %%
print(f"---PRICE DRIVER ANALYSIS ---")
print(f"Outcome: Modeled Octopus Agile prices with 89.2% accuracy.")
print(f"Key Insight: Demand 'Velocity' (Ramp) is a significant price driver.")
print(f"Price Impact: Every 1,000 MW of upward ramp adds ~2.8p to the unit rate.")
print(f"Statistical Note: Durbin-Watson (0.26) suggests a need for 'Time-of-Day' features.")

# %%


# %%
# Logic: Net Demand = ND - (Renewables)

print(f"---NET DEMAND BREAKTHROUGH ---")
print(f"Contribution: This is the primary driver of 'Plunge Pricing' and 'Peak Stress'.")

# Correlation Comparison
corr_nd = master_data['ND'].corr(master_data['value_inc_vat'])
corr_net = master_data['net_demand'].corr(master_data['value_inc_vat'])

print(f"Raw Demand Correlation with Price: {corr_nd:.3f}")
print(f"Net Demand Correlation with Price: {corr_net:.3f}")
print(f"Insight: Net Demand is a superior predictor because it accounts for 'Green Oversupply'.")

# %%

from sqlalchemy import create_engine

# 1. Create the connection string
# Format: postgresql://username:password@localhost:5432/database_name
engine = create_engine('postgresql://postgres:ramon123@localhost:5432/energy_warehouse')

# 2. Upload Normalized 'Silver' Tables
neso.to_sql('stg_neso_demand', engine, if_exists='replace', index=False)
octopus.to_sql('stg_octopus_prices', engine, if_exists='replace', index=False)
weather.to_sql('stg_weather_historical', engine, if_exists='replace', index=False)

# 3. Upload 'Gold' Master Table (The 90% Model Data)
# We keep index=True because your index is the 'time' column we fixed
master_data.to_sql('fct_grid_analysis', engine, if_exists='replace', index=True)

print("🚀 Successfully migrated all tables to PostgreSQL: energy_warehouse")

# %%


fig = plt.figure(layout="constrained", figsize=(15,8))

gs = GridSpec(3,3, figure = fig)

ax1 = plt.subplot(gs.new_subplotspec((0,0), colspan=1))
ax2 = plt.subplot(gs.new_subplotspec((0,1), colspan=1))
ax3 = plt.subplot(gs.new_subplotspec((1,0), colspan=1))
ax4 = plt.subplot(gs.new_subplotspec((1,1), colspan=1))


ax1.plot(sunny_week.index, sunny_week['z_demand'], label='Demand (Z-Score)', color='blue', alpha=0.7)
ax1.plot(sunny_week.index, sunny_week['z_radiation'], label='Radiation (Z-Score)', color='orange', alpha=0.5)

ax1.plot()
# %%
 
