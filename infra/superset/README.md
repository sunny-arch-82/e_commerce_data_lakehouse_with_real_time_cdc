# Superset Dashboard Design Guide for E-Commerce CDC Lakehouse

The E-Commerce CDC Lakehouse project has two core strengths that need to be showcased on the Dashboard:
1. **Real-time CDC Streaming**: Data flows from Postgres to the Dashboard with very low latency (thanks to Kafka + Spark 30s micro-batch).
2. **Medallion Architecture (dbt)**: Raw data is cleaned, transformed, and linked into a Star Schema standard at the `mart` layer, optimized for high-speed Trino queries.

Below are detailed steps to build an "Executive" Dashboard that truly reflects the system's capabilities.

---

## Step 1: Declare Datasets (Mart Layer)

Log into Superset (default: `http://localhost:8089` with credentials `admin`/`admin123`).
Go to **Data** -> **Datasets** -> **+ Dataset** and add the following tables from the `Trino` Database, `mart` Schema:

- `fct_orders`: General order information
- `fct_events`: User behavior tracking (clicks, views, add to cart...)
- `fct_order_items`: Detailed information for each product in an order
- `dim_products`: Product configuration information
- `dim_customers`: Customer demographics

---

## Step 2: Initialize Strategic Chart Groups

### Group 1: Showcasing Real-time Streaming
Purpose: Show numbers updating continuously every few dozen seconds without overnight batch-processing.

1. **Big Number - Total Revenue Today**
   - **Dataset**: `fct_order_items`
   - **Metric**: `SUM(sale_price)`
   - **Filter**: Time filter for `Today`.

2. **Time-series Line Chart - Live Traffic & Orders**
   - **Dataset**: `fct_events` (or `fct_orders`)
   - **Time Grain**: `Minute`
   - **Metric**: `COUNT(*)` (Number of visits / Number of new orders).

### Group 2: Showcasing Deep Data Transformation (dbt Transform)
Purpose: Prove that dbt has processed session grouping and user behavior logic smoothly.

3. **Funnel Chart - Conversion Funnel**
   - **Dataset**: `fct_events`
   - **Group by**: `event_type`
   - **Metric**: `COUNT(DISTINCT session_id)`
   - *How it works*: Draw an origin funnel from `Home` -> `Product` -> `Cart` -> `Purchase`.

4. **Pie Chart - Orders by Traffic Source**
   - **Dataset**: `fct_orders`
   - **Group by**: `traffic_source` (Organic, Facebook, Google, Email...)
   - **Metric**: `COUNT(order_id)`

### Group 3: Core Business Analytics (BI)
Purpose: Prove that data has been properly joined (Star schema) and cleaned.

5. **Horizontal Bar Chart - Top 10 Best Selling Products**
   - **Dataset**: Virtual dataset (SQL Lab) joining `fct_order_items` and `dim_products`
   - **Metric**: `SUM(sale_price)` and `COUNT(order_id)`
   - **Limit**: 10
   - **Sort**: Descending `SUM(sale_price)`.

6. **Bar Chart - Customers Demographics**
   - **Dataset**: `dim_customers`
   - **Group by**: `gender` and age group (`age_group`)
   - **Metric**: `COUNT(*)`

---

## Step 3: Dashboard Layout Guide (Grid Layout)

For a professional, scientific Dashboard that follows data analysis UI/UX principles (reading top to bottom, left to right), arrange the charts in a Grid layout as follows:

### Row 1: Top-level KPIs
*Place at the top for a quick overview of business status.*
- **Big Number - Total Revenue** - [Width: 1/4 screen]
- **Big Number - Total Orders** - [Width: 1/4 screen]
- **Big Number - Active Users** - [Width: 1/4 screen]
- **Big Number - Conversion Rate** - [Width: 1/4 screen]

### Row 2: Trends over Time
*Line charts need wide spaces to display time clearly without clutter.*
- **Time-series Line Chart (Live Traffic & Orders)** - [Width: Full 100% screen]
  - *Tip:* Keep the height moderate (about 300px - 400px) to avoid pushing other charts too far down.

### Row 3: Acquisition & Behavior
*Split screen to compare 2 customer perspectives.*
- **Funnel Chart (Conversion Funnel)** - [Width: 50% left]
  - *Reason:* On the left because User Flow is usually read from left to right.
- **Pie Chart (Orders by Traffic Source)** - [Width: 50% right]
  - *Reason:* Shows where the customers entering the funnel on the left came from.

### Row 4: Details & Demographics
*Specific details at the bottom for those who want deeper analysis.*
- **Horizontal Bar Chart (Top 10 Best Selling Products)** - [Width: 50% or 60% left]
  - *Reason:* Horizontal bars need width to clearly display long product names.
- **Stacked Bar Chart (Customers Demographics)** - [Width: 50% or 40% right]
  - *Reason:* Age and gender pyramids don't usually take up much horizontal space.

### Superset Tips

1. **Use Layout Elements (Right side of Edit Dashboard screen):**
   - Drag the **Row** element into the empty space first.
   - Drag charts into each Row to ensure they don't get misaligned when zooming.
2. **Add Header:**
   - Drag the **Header** element to the very top, name it `E-Commerce CDC Lakehouse - Realtime Executive Dashboard` for added credibility.

---

## KILLER FEATURE: Auto-Refresh (Most Important)

To prove this is a **Live Lakehouse**:
1. In the Dashboard screen, click the 3 dots `...` in the top right corner.
2. Select **Set auto-refresh interval**.
3. Set it to **30 seconds** (or 10s if preferred).

**Effect**: Every 30s, the charts will blink slightly and jump to new numbers as Spark Streaming finishes pushing a new micro-batch of data in the background. This is the most valuable detail to create a "Wow" effect when demoing the project!

### Group 4: Advanced Analytics
Purpose: Multi-dimensional data analysis to find deeper insights into revenue and customers.

7. **Pivot Table v2 (Cross-table with Heatmap color)**
   - **Purpose**: Analyze Revenue shifts of each Category across months.
   - **Dataset**: `fct_order_items` combined with `dim_products` (SQL Lab: Save as View/Dataset)
   - **Config**:
     - **Rows**: `category` (or `department`)
     - **Columns**: `created_at` (grain is `Month` or `Year`)
     - **Metrics**: `SUM(sale_price)`
     - Enable **Conditional Formatting** to create a Heatmap effect highlighting high/low values.

8. **Big Number with Trendline**
   - **Purpose**: Display this month's Revenue and daily fluctuations.
   - **Dataset**: `fct_order_items`
   - **Config**:
     - **Metrics**: `SUM(sale_price)`
     - **Time Column**: `created_at`
     - **Time Grain**: `Day`
     - *Effect*: Displays a very large total number on top and a line graph showing fluctuations below.

9. **Sunburst Chart**
   - **Purpose**: See the proportion of revenue going from Department -> Category -> Brand.
   - **Dataset**: Dataset combining `fct_order_items` and `dim_products`
   - **Config**:
     - **Hierarchy**: Drag columns sequentially: `department` ➔ `category` ➔ `brand`.
     - **Metrics**: `SUM(sale_price)`
     - *Effect*: Clicking on an outer layer (e.g. Men) automatically zooms into the inner subcategories.

10. **Grouped Bar Chart**
    - **Purpose**: Compare Revenue correlation between Top Countries over years.
    - **Dataset**: Dataset combining `fct_order_items` and `dim_customers`
    - **Config**:
      - **X-axis**: `created_at` (Year)
      - **Metrics**: `SUM(sale_price)`
      - **Dimensions (Group by)**: `country`
      - **Bar mode**: Select `Grouped` (Columns will stand next to each other instead of stacking).

---

## Step 4: Create "Master Dataset" (Virtual Dataset) in SQL Lab

To build cross-analysis charts like Group 4 (e.g. View revenue of Product A in Country B), you need a dataset that groups enough tables. Instead of writing dbt code to add a huge table, you can use Superset's Virtual Dataset feature:

1. On the top menu, select **SQL Lab** ➔ **SQL Editor**.
2. Select Database: `Trino`, Schema: `mart`.
3. Paste the following SQL code into the Editor:
   ```sql
   SELECT 
       i.order_item_id,
       i.order_id,
       i.item_status,
       i.sale_price,
       i.revenue,
       i.item_created_at,
       -- Product Info (Dim Products)
       p.product_category AS category,
       p.product_department AS department,
       p.product_brand AS brand,
       p.cost,
       -- Customer Info (Dim Customers)
       c.country,
       c.gender,
       c.age,
       c.customer_tier
   FROM mart.fct_order_items i
   LEFT JOIN mart.dim_products p ON i.product_id = p.product_id
   LEFT JOIN mart.dim_customers c ON i.user_id = c.user_id
   ```
4. Click **Run** to test.
5. When results appear, click **Save** ➔ Select **Save as Dataset**.
6. Name the Dataset: `Master_Order_Items_Analytics`.

Now when creating a Pivot Table or Sunburst chart, just select `Master_Order_Items_Analytics` as the data source. It will have all columns from `sale_price` to `country` and `category` ready for drag and drop!
