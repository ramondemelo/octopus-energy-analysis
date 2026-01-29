# ⚡ Octopus Energy: Agile Price & Demand Analysis (2025)

An end-to-end data analytics project exploring the drivers of the UK National Grid demand and its impact on Octopus Energy's "Agile" pricing structures.

## 📊 Executive Summary
This project analyzes 17,520 half-hourly settlement periods to identify why energy prices fluctuate. By merging weather data with National Grid (NESO) demand, I have modeled the "Physics" of the UK energy market.

## 🚀 Key Findings
### 1. The Modeling Evolution
I tested three stages of regression to move from basic correlation to high-accuracy forecasting:
* **Stage 1 (Weather Only):** Proved that a 1°C drop in temperature correlates to a **~434 MW increase** in national demand ($R^2$: 0.21).
* **Stage 2 (60-min Momentum):** Adding a 1-hour lag increased accuracy to **92.5%**.
* **Stage 3 (30-min Momentum):** Using a 30-minute lag reached **97.9% accuracy**, demonstrating that grid inertia is the primary driver of short-term demand.

### 2. The "Plunge Pricing" Phenomenon
* Identified **289 Plunge Events** (144.5 hours) where prices dropped below £0.00.
* **The Driver:** These events are highly correlated with "Strong" Solar Radiation (>450 $W/m^2$) and high wind speeds during low-demand periods.

## 🛠️ Feature Engineering
To achieve a professional-grade model, I implemented:
* **Autoregressive Lags:** Capturing the "momentum" of the grid.
* **Workday Flags:** Accounting for the industrial demand drop on weekends.
* **Standardization:** Using Z-Scores to compare variables with different units (MW vs °C).

## 📂 Project Structure
* `data/`: Raw and processed datasets (Ignored via .gitignore).
* `logic.py`: Main analysis script using Python/Pandas/Statsmodels.
* `README.md`: Project documentation and insights.

## 📈 Next Steps
- [ ] **Ramp Rate Analysis:** Calculate the "velocity" of demand during the evening peak.
- [ ] **SQL Migration:** Move features into a relational database for dashboarding.
- [ ] **Quarto Dashboard:** Build a live-style visual report of these insights.