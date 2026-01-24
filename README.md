# Commodities

Commodity futures returns and basis data from Bloomberg and WRDS.

## Overview

This pipeline computes monthly commodity returns using GSCI (Goldman Sachs Commodity Index) excess return indices from Bloomberg, matched to He-Kelly-Manela commodity factors using correlation-based optimal pairing.

## Commodities Covered

### Energy
- Crude Oil (Brent, WTI)
- Natural Gas
- Heating Oil
- Gasoline

### Metals
- Gold, Silver
- Copper, Aluminum
- Nickel, Lead, Zinc

### Agriculture
- Corn, Wheat, Soybeans
- Sugar, Coffee, Cocoa
- Cotton

### Livestock
- Lean Hogs
- Live Cattle
- Feeder Cattle

## Data Sources

- **Bloomberg**: GSCI excess return indices, commodity futures prices, LME metals
- **WRDS**: Futures contract data (tr_ds_fut)
- **He-Kelly-Manela**: Commodity factor reference for matching

## Outputs

- `ftsfr_commodities_returns.parquet`: Monthly commodity returns (unique_id, ds, y)

## Dependencies

This pipeline requires data from the `he_kelly_manela` repository for factor matching.

## Requirements

- Bloomberg Terminal (for GSCI indices and futures data)
- WRDS account (for futures contract data)
- he_kelly_manela repository data
- Python 3.10+

## Setup

1. Ensure Bloomberg Terminal is running
2. Configure WRDS credentials in `~/.pgpass`
3. Run he_kelly_manela pipeline first to generate reference data
4. Install dependencies: `pip install -r requirements.txt`
5. Run pipeline: `doit`

## Academic References

### Primary Papers

- **Yang (2013)** - "What Does the Yield Curve Tell Us About GDP Growth?"
  - Identifies slope factor in commodity futures cross-section
  - 10% annual return spread between high and low basis portfolios

- **He, Kelly, and Manela (2017)** - "Intermediary Asset Pricing"
  - Commodities as test assets for intermediary capital factor

### Key Findings

- High-basis commodity futures have higher expected returns
- The basis spread mirrors FX forward rate spreads
- Intermediary capital shocks explain commodity return variation
