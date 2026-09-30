"""
dashboard.py
-------------
Live dashboard for the Q-commerce demand sensing pipeline.

Reads the aggregated collections written by consumer.py (demand_by_zone,
demand_by_category, stock_snapshot) from MongoDB and renders 3 live charts
in the browser using Plotly.js (loaded from CDN, client-side only --
no extra Python plotting library needed).

Run this AFTER consumer.py has been running for a little while so there's
data to show. Then open http://localhost:5000 in your browser.
The page auto-refreshes its data every 5 seconds.
"""

from flask import Flask, jsonify, render_template_string
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "qcommerce"

app = Flask(__name__)
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

PAGE_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>Q-commerce Demand Sensing Dashboard</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/plotly.js/2.27.0/plotly.min.js"></script>
  <style>
    body { font-family: 'Segoe UI', Arial, sans-serif; background: #eef1f6; margin: 0; padding: 25px; }
    h1 { color: #1a1a2e; margin-bottom: 4px; }
    .status { color: #666; font-size: 13px; margin-bottom: 20px; }
    .stat-row { display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
    .stat-card { background: white; border-radius: 10px; padding: 14px 20px; flex: 1 1 200px;
                 box-shadow: 0 1px 4px rgba(0,0,0,0.12); }
    .stat-card .label { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }
    .stat-card .value { font-size: 24px; font-weight: 700; color: #1a1a2e; margin-top: 4px; }
    .charts { display: flex; flex-wrap: wrap; gap: 20px; }
    .chart-box { background: white; border-radius: 10px; padding: 12px;
                 box-shadow: 0 1px 6px rgba(0,0,0,0.15); flex: 1 1 45%; min-width: 420px; }
  </style>
</head>
<body>
  <h1>Q-commerce Demand Sensing — Live Dashboard</h1>
  <div class="status">Auto-refreshing every 5 seconds &middot; Last updated: <span id="ts"></span></div>

  <div class="stat-row" id="stat-row"></div>

  <div class="charts">
    <div class="chart-box"><div id="chart_zone"></div></div>
    <div class="chart-box"><div id="chart_category"></div></div>
    <div class="chart-box"><div id="chart_revenue"></div></div>
    <div class="chart-box" style="flex-basis: 100%;"><div id="chart_stock"></div></div>
  </div>

  <script>
    const BASE_LAYOUT = {
      margin: {t: 50, l: 60, r: 20, b: 90},
      font: {family: 'Segoe UI, Arial, sans-serif', size: 13, color: '#333'},
      paper_bgcolor: 'white',
      plot_bgcolor: 'white',
      xaxis: {tickangle: -20, automargin: true},
      yaxis: {gridcolor: '#eee', automargin: true},
      hoverlabel: {bgcolor: 'white', font: {size: 13}},
    };

    function sortDesc(labels, values) {
      const idx = values.map((v, i) => i).sort((a, b) => values[b] - values[a]);
      return { labels: idx.map(i => labels[i]), values: idx.map(i => values[i]) };
    }

    function renderStats(data) {
      const totalOrders = data.zone.values.reduce((a, b) => a + b, 0);
      const totalRevenue = data.revenue.values.reduce((a, b) => a + b, 0);
      const lowStockCount = data.stock.values.filter(v => v < 30).length;
      const topZoneIdx = data.zone.values.indexOf(Math.max(...data.zone.values));
      const topZone = data.zone.labels[topZoneIdx] || '—';

      const cards = [
        {label: 'Total Orders Processed', value: totalOrders.toLocaleString()},
        {label: 'Total Order Value (₹)', value: '₹' + totalRevenue.toLocaleString(undefined, {maximumFractionDigits: 0})},
        {label: 'Top Performing Zone', value: topZone},
        {label: 'SKUs Below 30 Units', value: lowStockCount},
      ];
      document.getElementById('stat-row').innerHTML = cards.map(c =>
        `<div class="stat-card"><div class="label">${c.label}</div><div class="value">${c.value}</div></div>`
      ).join('');
    }

    async function loadData() {
      const res = await fetch('/api/data');
      const data = await res.json();

      renderStats(data);

      const zoneSorted = sortDesc(data.zone.labels, data.zone.values);
      Plotly.newPlot('chart_zone', [{
        x: zoneSorted.labels, y: zoneSorted.values, type: 'bar',
        marker: {color: '#4C78A8', line: {color: '#2c4a6e', width: 1}},
        text: zoneSorted.values.map(String), textposition: 'outside',
        hovertemplate: '<b>%{x}</b><br>Orders: %{y}<extra></extra>'
      }], {...BASE_LAYOUT, title: {text: 'Orders by Zone', font: {size: 17}},
           xaxis: {...BASE_LAYOUT.xaxis, title: 'Zone'},
           yaxis: {...BASE_LAYOUT.yaxis, title: 'Number of Orders'}});

      const catSorted = sortDesc(data.category.labels, data.category.values);
      Plotly.newPlot('chart_category', [{
        x: catSorted.labels, y: catSorted.values, type: 'bar',
        marker: {color: '#F58518', line: {color: '#a85c0d', width: 1}},
        text: catSorted.values.map(String), textposition: 'outside',
        hovertemplate: '<b>%{x}</b><br>Orders: %{y}<extra></extra>'
      }], {...BASE_LAYOUT, title: {text: 'Orders by Category', font: {size: 17}},
           xaxis: {...BASE_LAYOUT.xaxis, title: 'Category'},
           yaxis: {...BASE_LAYOUT.yaxis, title: 'Number of Orders'}});

      const revSorted = sortDesc(data.revenue.labels, data.revenue.values);
      Plotly.newPlot('chart_revenue', [{
        x: revSorted.labels, y: revSorted.values, type: 'bar',
        marker: {color: '#72B7B2', line: {color: '#3f7a75', width: 1}},
        text: revSorted.values.map(v => '₹' + v.toLocaleString(undefined, {maximumFractionDigits: 0})),
        textposition: 'outside',
        hovertemplate: '<b>%{x}</b><br>Order Value: ₹%{y:,.2f}<extra></extra>'
      }], {...BASE_LAYOUT, title: {text: 'Order Value (₹) by Zone', font: {size: 17}},
           xaxis: {...BASE_LAYOUT.xaxis, title: 'Zone'},
           yaxis: {...BASE_LAYOUT.yaxis, title: 'Total Order Value (₹)'}});

      Plotly.newPlot('chart_stock', [{
        x: data.stock.labels, y: data.stock.values, type: 'bar',
        marker: {
          color: data.stock.values.map(v => v < 30 ? '#E45756' : '#54A24B'),
          line: {color: '#333', width: 0.5}
        },
        text: data.stock.values.map(String), textposition: 'outside',
        hovertemplate: '<b>%{x}</b><br>Stock Left: %{y} units<extra></extra>'
      }], {...BASE_LAYOUT, title: {text: 'Lowest Stock SKUs by Zone (red = below 30 units)', font: {size: 17}},
           xaxis: {...BASE_LAYOUT.xaxis, title: 'SKU (Zone)', tickangle: -30},
           yaxis: {...BASE_LAYOUT.yaxis, title: 'Units in Stock'}});

      document.getElementById('ts').innerText = new Date().toLocaleTimeString();
    }

    loadData();
    setInterval(loadData, 5000);
  </script>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(PAGE_HTML)


@app.route("/api/data")
def api_data():
    # Chart 1: Orders by zone
    zone_docs = list(db.demand_by_zone.find({}, {"_id": 0, "zone": 1, "order_count": 1}))
    zone_labels = [d["zone"] for d in zone_docs]
    zone_values = [d["order_count"] for d in zone_docs]

    # Chart 4: Order value (revenue) by zone -- same collection, different field
    revenue_docs = list(db.demand_by_zone.find({}, {"_id": 0, "zone": 1, "total_value": 1}))
    revenue_labels = [d["zone"] for d in revenue_docs]
    revenue_values = [round(d.get("total_value", 0), 2) for d in revenue_docs]

    # Chart 2: Orders by category
    cat_docs = list(db.demand_by_category.find({}, {"_id": 0, "category": 1, "order_count": 1}))
    cat_labels = [d["category"] for d in cat_docs]
    cat_values = [d["order_count"] for d in cat_docs]

    # Chart 3: Current stock per SKU (top 15 lowest-stock items -- most actionable view)
    stock_docs = list(
        db.stock_snapshot.find({}, {"_id": 0, "sku_name": 1, "zone": 1, "current_stock": 1})
        .sort("current_stock", 1)
        .limit(15)
    )
    stock_labels = [f"{d['sku_name']} ({d['zone']})" for d in stock_docs]
    stock_values = [d["current_stock"] for d in stock_docs]

    return jsonify({
        "zone": {"labels": zone_labels, "values": zone_values},
        "category": {"labels": cat_labels, "values": cat_values},
        "revenue": {"labels": revenue_labels, "values": revenue_values},
        "stock": {"labels": stock_labels, "values": stock_values},
    })


if __name__ == "__main__":
    print("[dashboard] Starting on http://localhost:5000")
    print("[dashboard] Make sure consumer.py has been running for a bit so there's data to show.")
    app.run(debug=False, port=5000)