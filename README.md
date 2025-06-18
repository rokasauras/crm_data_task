
<body>
    <h1>Data Executive SQL Test</h1>

  <p>Here you can find a task I've been given that includes some raw data sets, and I've had to clean and extract data from the sources and submit everything within the hour.<br>
    My comments as to how I did it are as follows, thank you for your time.</p>
    <p><strong>— Data Executive SQL Test</strong></p>

  <hr>

  <h2>Task Steps &amp; Approach</h2>
    <p>You can find the steps I'm documenting to complete this task below. I've explored the data for a few minutes so I can understand what I'm looking at.</p>

  <hr>

  <h3>1. Create the Database &amp; Import CSVs</h3>
    <p>First I'll create a new database in SQLiteStudio called <code>crm_data.db</code>, and import the three csv files:</p>
    <ul>
        <li><code>finance_crm_raw_data.csv</code></li>
        <li><code>sales_crm_raw_data.csv</code></li>
        <li><code>operational_crm_raw_data.csv</code></li>
    </ul>

  <hr>

  <h2>Finance CRM</h2>

  <h3>Check for Duplicates</h3>
    <pre><code class="language-sql">SELECT
  customer_id,
  COUNT(*) AS times_found
FROM 
  finance_crm_raw_data
GROUP BY
  customer_id
HAVING
  COUNT(*) &gt; 1;</code></pre>
    <p>No customer_id duplicates, so no further check is needed.</p>

  <h3>Create Clean Table</h3>
    <pre><code class="language-sql">CREATE TABLE finance_crm_clean (
  customer_id     INTEGER,
  company_name    TEXT,
  amount_billed   REAL,
  billing_date    DATE,
  payment_status  TEXT
);</code></pre>

  <h3>Insert Cleaned Data</h3>
    <pre><code class="language-sql">INSERT INTO finance_crm_clean
SELECT
  CAST(customer_id AS INTEGER),
  company_name,
  CAST(amount_billed AS REAL),
  SUBSTR(billing_date, 7, 4) || '-' || SUBSTR(billing_date, 4, 2) || '-' || SUBSTR(billing_date, 1, 2),
  payment_status
FROM finance_crm_raw_data;</code></pre>

  <hr>

  <h2>Sales CRM</h2>

  <h3>Check for Duplicates</h3>
    <pre><code class="language-sql">SELECT 
  customer_id,
  COUNT(*) AS count
FROM 
  sales_crm_raw_data
GROUP BY 
  customer_id
HAVING
  COUNT(*) &gt; 1;</code></pre>

  <h3>Create Clean Table</h3>
    <pre><code class="language-sql">CREATE TABLE sales_crm_clean (
  customer_id         INTEGER,
  company_name        TEXT,
  sales_status        TEXT,
  region              TEXT,
  lead_source         TEXT,
  won_date            DATE,
  plan_type           TEXT,
  first_year_revenue  REAL,
  contracted_months   INTEGER
);</code></pre>

  <h3>Insert Cleaned Data</h3>
    <pre><code class="language-sql">INSERT INTO sales_crm_clean
SELECT
  customer_id,
  company_name,
  sales_status,
  region,
  lead_source,
  SUBSTR(won_date, 7, 4) || '-' || SUBSTR(won_date, 4, 2) || '-' || SUBSTR(won_date, 1, 2),
  plan_type,
  CAST(first_year_revenue AS REAL),
  CAST(contracted_months AS INTEGER)
FROM sales_crm_raw_data;</code></pre>

  <h3>Standardise Values</h3>
    <pre><code class="language-sql">UPDATE sales_crm_clean
SET
  sales_status = UPPER(sales_status),
  plan_type = UPPER(plan_type);</code></pre>

  <hr>

  <h2>Operational CRM</h2>

  <h3>JSON Parsing &amp; Data Cleaning</h3>
    <p>This one I'll need to parse a JSON. I'm not really sure which created_date to use, as there are conflicting columns, but I'll use the main one provided. Sometimes the date opened is after the date closed, but I'll stick to the structure for now.</p>

  <h3>Create Clean Table</h3>
    <pre><code class="language-sql">CREATE TABLE operational_crm_clean (
  record_id     INTEGER,
  customer_id   INTEGER,
  ticket_status TEXT,
  ticket_type   TEXT,
  source        TEXT,
  assigned_to   TEXT,
  created_date  DATE,
  closed_date   DATE
);</code></pre>

  <h3>Insert Parsed Data</h3>
    <pre><code class="language-sql">INSERT INTO operational_crm_clean (record_id, customer_id, ticket_status, ticket_type, source, assigned_to, created_date, closed_date)
SELECT
  CAST(json_extract(fields, '$.Record_ID') AS INTEGER),
  CAST(json_extract(fields, '$."Customer ID"') AS INTEGER),
  json_extract(fields, '$."Ticket Status"'),
  json_extract(fields, '$."Ticket Type"'),
  json_extract(fields, '$.Source'),
  json_extract(fields, '$."Assigned to"'),
  json_extract(fields, '$."Date Created"'),
  json_extract(fields, '$."Date Closed"')
FROM operational_crm_raw_data;</code></pre>

  <h3>Remove Duplicates</h3>
    <pre><code class="language-sql">DELETE FROM operational_crm_clean
WHERE rowid NOT IN (
  SELECT MIN(rowid)
  FROM operational_crm_clean
  GROUP BY record_id, customer_id, ticket_status, ticket_type, source, assigned_to, created_date, closed_date
);</code></pre>

  <hr>

  <h2>Data Marts</h2>

  <h3>Sales Data Mart</h3>
    <pre><code class="language-sql">CREATE TABLE sales_data_mart AS
SELECT
  s.customer_id,
  s.company_name AS full_name,
  NULL AS email, -- No email data available
  s.sales_status,
  s.region,
  s.lead_source,
  s.won_date,
  s.plan_type,
  COALESCE(o.complaint_count, 0) AS count_of_complaints_per_customer
FROM
  sales_crm_clean s
LEFT JOIN (
  SELECT
    customer_id,
    COUNT(*) AS complaint_count
  FROM
    operational_crm_clean
  WHERE
    ticket_type = 'Complaint'
  GROUP BY
    customer_id
) o ON s.customer_id = o.customer_id;</code></pre>

  <h3>Finance Data Mart</h3>
    <pre><code class="language-sql">CREATE TABLE finance_data_mart AS
SELECT
  f.customer_id,
  s.plan_type,
  f.amount_billed,
  f.billing_date,
  f.payment_status,
  s.region,
  s.first_year_revenue,
  (s.first_year_revenue - f.amount_billed) AS difference_revenue_billed
FROM
  finance_crm_clean f
LEFT JOIN sales_crm_clean s ON f.customer_id = s.customer_id;</code></pre>

  <h3>Operations Data Mart</h3>
    <pre><code class="language-sql">CREATE TABLE operations_data_mart AS
SELECT
  o.customer_id,
  o.record_id,
  s.company_name AS full_name,
  NULL AS email, -- Email data is not available
  o.ticket_type,
  s.won_date,
  s.plan_type,
  o.created_date,
  o.closed_date,
  o.assigned_to,
  f.payment_status,
  JULIANDAY(o.closed_date) - JULIANDAY(o.created_date) AS Number_of_days_to_close_ticket
FROM
  operational_crm_clean o
LEFT JOIN sales_crm_clean s ON o.customer_id = s.customer_id
LEFT JOIN finance_crm_clean f ON o.customer_id = f.customer_id;</code></pre>
</body>
</html>
