import os
import csv
import re
from datetime import datetime
from typing import List, Dict, Any
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SHEET_ID = "1Js9z9wjn0sSdZMhR-qhpCp6mnsrxPvplYN0V1dsBxqA"
CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

CURRENCY_RE = re.compile(r"[^0-9.\-]")


def to_float(val: str) -> float:
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return 0.0
    # Remove currency symbols and commas
    s = CURRENCY_RE.sub("", s)
    try:
        return float(s) if s else 0.0
    except ValueError:
        return 0.0


def parse_date(val: str) -> datetime | None:
    if not val:
        return None
    s = str(val).strip()
    # Try common formats
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None


@app.get("/")
def read_root():
    return {"message": "Hello from FastAPI Backend!"}


@app.get("/api/hello")
def hello():
    return {"message": "Hello from the backend API!"}


@app.get("/test")
def test_database():
    """Test endpoint to check if database is available and accessible"""
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }

    try:
        # Try to import database module
        from database import db

        if db is not None:
            response["database"] = "✅ Available"
            response["database_url"] = "✅ Configured"
            response["database_name"] = db.name if hasattr(db, 'name') else "✅ Connected"
            response["connection_status"] = "Connected"

            # Try to list collections to verify connectivity
            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]  # Show first 10 collections
                response["database"] = "✅ Connected & Working"
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"

    except ImportError:
        response["database"] = "❌ Database module not found (run enable-database first)"
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"

    # Check environment variables
    import os
    response["database_url"] = "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set"
    response["database_name"] = "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set"

    return response


@app.get("/api/analysis")
def get_analysis() -> Dict[str, Any]:
    """Fetch Google Sheet CSV and compute KPI + chart datasets.
    Columns expected:
    CustomerID, CustomerName, Description, Category, Quantity, InvoiceDate, UnitPrice, Amount,
    Country, PaymentMode, DeliveryStatus, Email, EMIPlan, EMITotalMonths, EMIAmount,
    NextDueDate, TotalOrders, TotalSpend
    """
    try:
        r = requests.get(CSV_URL, timeout=10)
        if r.status_code != 200:
            raise HTTPException(status_code=502, detail="Failed to fetch Google Sheet")
        content = r.text
    except Exception as e:
        # Provide example fallback so UI still works
        return {
            "source": "fallback",
            "kpis": {
                "totalRevenue": 123456,
                "thisMonthRevenue": 8940,
                "avgGrowth": 10.4,
                "avgQtyPerMonth": 245,
            },
            "revenueTrend": [
                {"month": "Jan", "value": 22000},
                {"month": "Feb", "value": 26000},
                {"month": "Mar", "value": 31000},
                {"month": "Apr", "value": 28000},
            ],
            "engagementTrend": [
                {"month": "Jan", "value": 18000},
                {"month": "Feb", "value": 21500},
                {"month": "Mar", "value": 29000},
            ],
            "countrySales": [
                {"name": "Country A", "value": 40},
                {"name": "Country B", "value": 25},
                {"name": "Country C", "value": 15},
                {"name": "Country D", "value": 10},
                {"name": "Country E", "value": 10},
            ],
            "paymentDistribution": [
                {"name": "UPI / Digital Wallets", "value": 70},
                {"name": "Credit Card", "value": 15},
                {"name": "Debit Card", "value": 10},
                {"name": "Cash", "value": 5},
            ],
        }

    # Parse CSV
    rows: List[Dict[str, str]] = []
    reader = csv.DictReader(content.splitlines())
    for row in reader:
        rows.append(row)

    if not rows:
        raise HTTPException(status_code=422, detail="No rows in sheet")

    # Aggregations
    total_revenue = 0.0
    qty_per_month: Dict[str, int] = {}
    revenue_per_month: Dict[str, float] = {}
    engagement_per_month: Dict[str, float] = {}
    country_sales: Dict[str, float] = {}
    payment_counts: Dict[str, int] = {}

    months_set: set[str] = set()

    for rrow in rows:
        amount = to_float(rrow.get("Amount", "0"))
        qty = int(to_float(rrow.get("Quantity", "0")))
        total_orders = to_float(rrow.get("TotalOrders", "0"))
        country = (rrow.get("Country") or "Unknown").strip() or "Unknown"
        payment = (rrow.get("PaymentMode") or "Unknown").strip() or "Unknown"
        d = parse_date(rrow.get("InvoiceDate", ""))
        if d is None:
            # try to recover from formats like 2025-07-26 etc
            try:
                d = datetime.fromisoformat((rrow.get("InvoiceDate", "") or "").strip())
            except Exception:
                d = None
        # Month key as YYYY-MM
        if d is not None:
            mkey = d.strftime("%Y-%m")
        else:
            # fallback bucket
            mkey = "unknown"
        months_set.add(mkey)

        total_revenue += amount
        qty_per_month[mkey] = qty_per_month.get(mkey, 0) + qty
        revenue_per_month[mkey] = revenue_per_month.get(mkey, 0.0) + amount
        engagement_per_month[mkey] = engagement_per_month.get(mkey, 0.0) + total_orders
        country_sales[country] = country_sales.get(country, 0.0) + amount
        payment_counts[payment] = payment_counts.get(payment, 0) + 1

    # Determine current and previous month based on max month key
    valid_months = [m for m in months_set if m != "unknown"]
    valid_months.sort()
    now_month = valid_months[-1] if valid_months else None
    prev_month = valid_months[-2] if len(valid_months) >= 2 else None

    this_month_revenue = revenue_per_month.get(now_month or "", 0.0)
    last_month_revenue = revenue_per_month.get(prev_month or "", 0.0)
    avg_growth = 0.0
    if last_month_revenue > 0:
        avg_growth = ((this_month_revenue - last_month_revenue) / last_month_revenue) * 100.0

    # Avg qty per month
    month_count = max(1, len(valid_months))
    total_qty = sum(qty_per_month.get(m, 0) for m in valid_months)
    avg_qty_per_month = total_qty / month_count if month_count > 0 else 0

    # Build revenue trend sorted by month label like Jan, Feb
    def month_label(mkey: str) -> str:
        if mkey and len(mkey) == 7 and mkey != "unknown":
            dt = datetime.strptime(mkey + "-01", "%Y-%m-%d")
            return dt.strftime("%b")
        return "N/A"

    trend = [
        {"month": month_label(m), "value": round(revenue_per_month[m], 2)}
        for m in valid_months[-4:]  # last up to 4 months
    ]

    engagement = [
        {"month": month_label(m), "value": round(engagement_per_month.get(m, 0.0), 2)}
        for m in valid_months[-3:]  # last up to 3 months
    ]

    country_list = [
        {"name": k, "value": round(v, 2)} for k, v in sorted(country_sales.items(), key=lambda x: -x[1])
    ]

    payment_list = [
        {"name": k, "value": v} for k, v in sorted(payment_counts.items(), key=lambda x: -x[1])
    ]

    return {
        "source": "sheet",
        "kpis": {
            "totalRevenue": round(total_revenue, 2),
            "thisMonthRevenue": round(this_month_revenue, 2),
            "avgGrowth": round(avg_growth, 2),
            "avgQtyPerMonth": round(avg_qty_per_month, 2),
        },
        "revenueTrend": trend,
        "engagementTrend": engagement,
        "countrySales": country_list,
        "paymentDistribution": payment_list,
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
