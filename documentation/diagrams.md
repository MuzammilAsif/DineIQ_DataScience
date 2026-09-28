# DineIQ Analytics — Diagrams

All diagrams are Mermaid, so GitHub renders them in place. PNG renders for the report and slides are in `documentation/diagrams/`. Table and column names match `documentation/data_dictionary.md`. Process and file names match the code.

## 1. Architecture

```mermaid
flowchart LR
    GEN["data_generator/<br/>generate_dataset.py"] --> RAW[("raw_data/<br/>13 CSVs")]
    RAW --> S1["Spark 01<br/>ingest + validate"]
    S1 --> DQ["reports/<br/>data_quality_report.md"]
    RAW --> S2["Spark 02<br/>clean + quarantine"]
    S2 --> PROC[("processed_data/<br/>clean CSVs")]
    PROC --> S3["Spark 03 + spark_sql/<br/>integration_queries.sql"]
    S3 --> PQ[("parquet_data/<br/>fact_order_line, orders, ...")]
    PQ --> S4["Spark 04<br/>feature engineering"]
    S4 --> PQ
    PQ --> S5["Spark 05-06<br/>menu classification + MLlib"]
    PQ --> S6["Spark 07-14<br/>analytics modules"]
    PQ --> S15["Spark 15<br/>demand forecast (GBT)"]
    S5 --> MOD[("models/spark/")]
    S6 --> MOD
    S15 --> MOD
    S5 --> PQ
    S6 --> PQ
    S15 --> PQ
    PROC --> PY["python_pipeline/<br/>pandas + scikit-learn"]
    PY --> PMOD[("models/python/")]
    PQ --> CMP["compare_pipelines.py"]
    PMOD --> CMP
    CMP --> REP["reports/<br/>dual_pipeline_comparison_*"]
    PQ --> REC["recommendation_engine.py<br/>what_if_simulator.py"]
    REC --> PQ
    PQ --> APP["Streamlit app<br/>src/app.py"]
    MOD --> APP
    DB[("database/app.db<br/>users, audit_log, master data")] <--> APP
    APP --> USERS(["Administrator / Regional Manager /<br/>Restaurant Manager / Analyst"])
```

## 2. Entity-relationship diagram (13 tables: 11 entities + 2 bridge tables)

```mermaid
erDiagram
    Restaurants {
        string location_id PK
        string name
        string city
        string region
        string address
        string location_type
        int seating_capacity
        date opening_date
        float latitude
        float longitude
    }
    Menu_Categories {
        string category_id PK
        string category_name
        string description
    }
    Menu_Items {
        string item_id PK
        string item_name
        string category_id FK
        decimal base_price
        decimal base_cost
        string description
        bool is_active
        date launch_date
        bool is_seasonal
        string season
    }
    Pricing_History {
        string price_id PK
        string item_id FK
        string location_id FK "null = all locations"
        decimal price
        decimal cost
        date effective_start_date
        date effective_end_date
        string change_reason
    }
    Customers {
        string customer_id PK
        date signup_date
        string age_group
        string gender
        string home_location_id FK
        string preferred_channel
        bool loyalty_member
    }
    Promotions {
        string promotion_id PK
        string promotion_name
        string promotion_type
        decimal discount_value
        date start_date
        date end_date
        decimal min_order_value
    }
    Promotion_Items {
        string promotion_id FK
        string item_id FK
    }
    Promotion_Locations {
        string promotion_id FK
        string location_id FK
    }
    Orders {
        string order_id PK
        string customer_id FK
        string location_id FK
        timestamp order_datetime
        string order_channel
        string order_status
        string promotion_id FK
        decimal subtotal
        decimal discount_amount
        decimal tax_amount
        decimal total_amount
        string payment_method
    }
    Order_Items {
        string order_item_id PK
        string order_id FK
        string item_id FK
        int quantity
        decimal unit_price
        decimal unit_cost
        decimal discount_amount
        decimal line_total
    }
    Ratings {
        string rating_id PK
        string customer_id FK
        string item_id FK "null = overall visit rating"
        string location_id FK
        string order_id FK
        int rating_value
        date rating_date
        string review_text
    }
    Inventory {
        string inventory_id PK
        string item_id FK
        string location_id FK
        date date
        decimal opening_stock
        decimal received_stock
        decimal prepared_quantity
        decimal consumed_stock
        decimal closing_stock
        string unit
    }
    Wastage {
        string wastage_id PK
        string item_id FK
        string location_id FK
        date date
        decimal quantity_wasted
        string unit
        decimal cost_of_waste
        string reason
    }

    Menu_Categories ||--o{ Menu_Items : groups
    Menu_Items ||--o{ Pricing_History : "priced by"
    Restaurants |o--o{ Pricing_History : "location override"
    Restaurants |o--o{ Customers : "home location"
    Customers ||--o{ Orders : places
    Restaurants ||--o{ Orders : receives
    Promotions |o--o{ Orders : "applied to"
    Orders ||--o{ Order_Items : contains
    Menu_Items ||--o{ Order_Items : "sold as"
    Promotions ||--o{ Promotion_Items : covers
    Menu_Items ||--o{ Promotion_Items : "eligible in"
    Promotions ||--o{ Promotion_Locations : "runs at"
    Restaurants ||--o{ Promotion_Locations : hosts
    Customers ||--o{ Ratings : writes
    Orders ||--o{ Ratings : "rated in"
    Restaurants ||--o{ Ratings : "rated at"
    Menu_Items |o--o{ Ratings : "rated item"
    Menu_Items ||--o{ Inventory : stocked
    Restaurants ||--o{ Inventory : stocks
    Menu_Items ||--o{ Wastage : wasted
    Restaurants ||--o{ Wastage : records
```

## 3. Data-flow diagram, level 0

```mermaid
flowchart LR
    SRC["Restaurant operations data<br/>(generated POS, inventory,<br/>wastage and rating records)"]
    RM(["Restaurant Manager"])
    RG(["Regional Manager"])
    AN(["Analyst"])
    AD(["Administrator"])
    SYS(("DineIQ Analytics<br/>system"))

    SRC -- "13 raw CSV tables" --> SYS
    RM -- "login, filters, what-if inputs" --> SYS
    SYS -- "own-location dashboards,<br/>recommendations, estimates" --> RM
    RG -- "login, filters" --> SYS
    SYS -- "chain dashboards, model comparison,<br/>CSV and report exports" --> RG
    AN -- "login, filters, what-if inputs" --> SYS
    SYS -- "analytics, forecasts, model results,<br/>exports" --> AN
    AD -- "users, master data, registry refresh" --> SYS
    SYS -- "job status, audit log,<br/>model registry" --> AD
```

## 4. Data-flow diagram, level 1

```mermaid
flowchart TB
    SRC["Operations data source"]
    USER(["Dashboard users"])
    ADMIN(["Administrator"])

    P1["1.0 Data ingestion<br/>+ validation<br/>(Spark 01)"]
    P2["2.0 Cleaning<br/>(Spark 02)"]
    P3["3.0 Integration +<br/>Parquet storage<br/>(Spark 03, Spark SQL)"]
    P4["4.0 Feature<br/>engineering<br/>(Spark 04)"]
    P5["5.0 Classification, ML<br/>and analytics<br/>(Spark 05-15, Python pipeline)"]
    P6["6.0 Recommendation<br/>engine + what-if"]
    P7["7.0 Dashboard<br/>(Streamlit)"]

    D1[("D1 raw_data")]
    D2[("D2 processed_data<br/>+ quarantine")]
    D3[("D3 parquet_data")]
    D4[("D4 models")]
    D5[("D5 reports")]
    D6[("D6 app.db")]

    SRC --> D1
    D1 --> P1
    P1 -- "data-quality findings" --> D5
    D1 --> P2
    P2 -- "clean rows / quarantined rows" --> D2
    P2 -- "cleaning log" --> D5
    D2 --> P3
    P3 -- "fact_order_line, partitioned tables" --> D3
    D3 --> P4
    P4 -- "item, customer, location features" --> D3
    D3 --> P5
    D2 -- "Python pipeline input" --> P5
    P5 -- "classes, segments, forecasts,<br/>anomalies, basket rules" --> D3
    P5 -- "fitted models" --> D4
    P5 -- "analysis reports" --> D5
    D3 --> P6
    P6 -- "recommendations" --> D3
    P6 -- "recommendations report" --> D5
    D3 --> P7
    D4 --> P7
    D5 --> P7
    P7 <-- "users, audit log, master data" --> D6
    USER <-- "filters in, charts and exports out" --> P7
    ADMIN <-- "admin actions" --> P7
```

## 5. Use-case diagram

Mermaid has no native use-case diagram, so this uses a flowchart: actors on the left, use cases inside the system boundary.

```mermaid
flowchart LR
    RM(["Restaurant Manager"])
    RG(["Regional Manager"])
    AN(["Analyst"])
    AD(["Administrator"])

    subgraph SYS["DineIQ Analytics"]
        UC1(["Log in / log out"])
        UC2(["View dashboards<br/>(Executive, Menu, Customer,<br/>Forecast, Wastage, Anomalies)"])
        UC3(["Apply filters<br/>(date, location, category, class)"])
        UC4(["View recommendations"])
        UC5(["Run what-if scenario"])
        UC6(["Export CSV / download reports"])
        UC7(["View Spark vs Python<br/>model comparison"])
        UC8(["Manage users"])
        UC9(["Manage master data<br/>(restaurants, menu items)"])
        UC10(["Monitor jobs, model registry,<br/>audit log"])
    end

    RM --- UC1 & UC2 & UC3 & UC4 & UC5 & UC6
    RG --- UC1 & UC2 & UC3 & UC4 & UC5 & UC6 & UC7
    AN --- UC1 & UC2 & UC3 & UC4 & UC5 & UC6 & UC7
    AD --- UC1 & UC2 & UC3 & UC4 & UC5 & UC6 & UC7 & UC8 & UC9 & UC10
```

A Restaurant Manager's location filter is locked to their assigned restaurant.

## 6. Activity diagram: running a what-if scenario

Follows `src/pages/8_WhatIf_Simulator.py` and `python_pipeline/what_if_simulator.py`.

```mermaid
flowchart TD
    A([Start]) --> B{Logged in?}
    B -- no --> B1[Show sign-in page] --> Z([End])
    B -- yes --> C[Filter menu items by<br/>category and class filters]
    C --> D{Any items left?}
    D -- no --> D1[Show 'no items match'] --> Z
    D -- yes --> E[User picks item and scenario]
    E --> F["load_baseline(item):<br/>price, cost, sales, wastage,<br/>elasticity, forecast demand"]
    F --> G[User sets scenario input<br/>e.g. price change %]
    G --> H["Run scenario formula<br/>(no model is re-run)"]
    H --> I{Inputs changed since<br/>last run?}
    I -- yes --> J[Write what_if entry<br/>to audit_log]
    I -- no --> K
    J --> K["Show 'Simulated estimate,<br/>not an actual result'"]
    K --> L[Show baseline vs simulated:<br/>revenue, margin, demand,<br/>wastage, profitability]
    L --> M{Flags raised?<br/>e.g. stockout risk}
    M -- yes --> N[Show flags] --> O
    M -- no --> O[Show limitations and assumptions]
    O --> Z
```

## 7. Sequence diagram: login, dashboard load, filter

```mermaid
sequenceDiagram
    actor U as User
    participant B as Browser
    participant A as app.py (st.navigation)
    participant Au as auth.py
    participant DB as SQLite app.db
    participant P as Dashboard page
    participant DL as data_loader.py (st.cache_data)
    participant PQ as parquet_data/

    U->>B: open app, enter username and password
    B->>A: submit sign-in form
    A->>Au: login(username, password)
    Au->>DB: get_user(username)
    DB-->>Au: user row with scrypt hash
    Au->>Au: check_password_hash
    Au->>DB: audit_log: login_success / login_failed
    Au-->>A: role stored in session
    A->>A: build menu for this role
    A->>P: run default page (Executive Dashboard)
    P->>Au: require(allowed roles)
    P->>DL: orders(), wastage(), forecast(), ...
    DL->>PQ: read Parquet (first call only)
    PQ-->>DL: tables
    DL-->>P: cached DataFrames
    P-->>B: KPI tiles, charts, cards, source footers
    U->>B: change location filter
    B->>P: rerun with new filter state
    P->>DL: same loaders (served from cache)
    P->>P: filter_df(date, location, ...)
    P-->>B: redrawn KPIs and charts
```
