-- App-level data only. Analytical data stays in parquet_data/.

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL,            -- Administrator, Regional Manager, Restaurant Manager, Analyst
  full_name TEXT,
  assigned_location_id TEXT,     -- scopes a Restaurant Manager's views
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  action TEXT NOT NULL,
  details TEXT,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Master-data copies edited from the Admin page, seeded from processed_data/.
CREATE TABLE IF NOT EXISTS restaurants (
  location_id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  city TEXT NOT NULL,
  region TEXT NOT NULL,
  address TEXT,
  location_type TEXT,
  seating_capacity INTEGER,
  opening_date TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS menu_items (
  item_id TEXT PRIMARY KEY,
  item_name TEXT NOT NULL,
  category_id TEXT NOT NULL,
  base_price REAL NOT NULL,
  base_cost REAL NOT NULL,
  description TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  launch_date TEXT,
  is_seasonal INTEGER NOT NULL DEFAULT 0,
  season TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
