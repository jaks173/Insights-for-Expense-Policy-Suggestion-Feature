-- PART 1: Create Tables

CREATE TABLE orgs (
  id TEXT PRIMARY KEY,
  org_name TEXT NOT NULL,
  domain TEXT
);

CREATE TABLE employees (
  id TEXT PRIMARY KEY,
  details JSONB,
  org_id TEXT REFERENCES orgs(id),
  roles TEXT
);

CREATE TABLE expenses (
  id TEXT PRIMARY KEY,
  employee_id TEXT REFERENCES employees(id),
  org_id TEXT REFERENCES orgs(id),
  amount NUMERIC,
  created_at DATE,
  source TEXT
);

CREATE TABLE expense_policies (
  id TEXT PRIMARY KEY,
  org_id TEXT REFERENCES orgs(id),
  policy_type TEXT,
  description TEXT
);

-- PART 2: \COPY Load Commands (to run manually in psql)

-- \COPY orgs(id, org_name, domain) FROM 'orgs.csv' DELIMITER ',' CSV HEADER;
-- \COPY employees(id, details, org_id, roles) FROM 'employees.csv' DELIMITER ',' CSV HEADER;
-- \COPY expenses(id, employee_id, org_id, amount, created_at, source) FROM 'expenses.csv' DELIMITER ',' CSV HEADER;
-- \COPY expense_policies(id, org_id, policy_type, description) FROM 'expense_policies.csv' DELIMITER ',' CSV HEADER;

-- PART 3: Final Summary Query

WITH real_orgs AS (
  SELECT * FROM orgs
  WHERE LOWER(org_name) NOT LIKE '%test%'
    AND LOWER(org_name) NOT LIKE '%demo%'
    AND LOWER(org_name) NOT LIKE '%fyle%'
    AND LOWER(domain) NOT LIKE '%@gmail.com%'
),

march_expenses_all AS (
  SELECT * FROM expenses
  WHERE created_at >= '2025-03-01' AND created_at < '2025-04-01'
),

march_corp_card_expenses AS (
  SELECT * FROM march_expenses_all
  WHERE source = 'CORPORATE_CARD'
),

ccc_expense_count AS (
  SELECT org_id, COUNT(*) AS no_of_expenses_in_march
  FROM march_corp_card_expenses
  GROUP BY org_id
),

admin_users AS (
  SELECT
    org_id,
    STRING_AGG(id::TEXT, ',') AS admin_user_ids,
    STRING_AGG((details::jsonb ->> 'email'), ',') AS admin_emails,
    COUNT(*) AS admin_user_count
  FROM employees
  WHERE LOWER(roles) LIKE '%admin%'
  GROUP BY org_id
),

non_admin_users AS (
  SELECT org_id, COUNT(*) AS non_admin_user_count
  FROM employees
  WHERE LOWER(roles) NOT LIKE '%admin%'
  GROUP BY org_id
),

active_users AS (
  SELECT
    e.org_id,
    COUNT(DISTINCT e.id) AS no_of_active_users
  FROM employees e
  JOIN march_expenses_all ex ON e.id = ex.employee_id AND e.org_id = ex.org_id
  WHERE (details::jsonb ->> 'is_enabled')::BOOLEAN = TRUE
  GROUP BY e.org_id
),

policy_summary AS (
  SELECT
    org_id,
    CASE WHEN COUNT(id) > 0 THEN 'Yes' ELSE 'No' END AS expense_policies_present,
    COUNT(id) AS no_of_expense_policies_created
  FROM expense_policies
  GROUP BY org_id
),

total_expense_amount AS (
  SELECT org_id, SUM(amount) AS total_expense_amount
  FROM march_expenses_all
  GROUP BY org_id
),

total_users_who_expensed AS (
  SELECT org_id, COUNT(DISTINCT employee_id) AS total_users_who_expensed
  FROM march_expenses_all
  GROUP BY org_id
),

most_common_expense_type AS (
  SELECT org_id, source AS most_common_expense_type
  FROM (
    SELECT org_id, source,
           ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY COUNT(*) DESC) AS rnk
    FROM march_expenses_all
    GROUP BY org_id, source
  ) sub
  WHERE rnk = 1
),

total_expense_types AS (
  SELECT org_id, COUNT(DISTINCT source) AS total_expense_types
  FROM march_expenses_all
  GROUP BY org_id
)

SELECT
  ro.id AS org_id,
  ro.org_name,
  COALESCE(au.admin_user_ids, '') AS admin_user_ids,
  COALESCE(au.admin_emails, '') AS admin_emails,
  COALESCE(au.admin_user_count, 0) AS admin_user_count,
  COALESCE(nau.non_admin_user_count, 0) AS non_admin_user_count,
  COALESCE(ec.no_of_expenses_in_march, 0) AS no_of_expenses_in_march,
  COALESCE(ps.expense_policies_present, 'No') AS expense_policies_present,
  COALESCE(ps.no_of_expense_policies_created, 0) AS no_of_expense_policies_created,
  COALESCE(au2.no_of_active_users, 0) AS no_of_active_users,
  COALESCE(te.total_expense_amount, 0) AS total_expense_amount,
  COALESCE(tu.total_users_who_expensed, 0) AS total_users_who_expensed,
  COALESCE(mc.most_common_expense_type, '') AS most_common_expense_type,
  COALESCE(tt.total_expense_types, 0) AS total_expense_types
FROM real_orgs ro
LEFT JOIN admin_users au ON ro.id = au.org_id
LEFT JOIN non_admin_users nau ON ro.id = nau.org_id
LEFT JOIN ccc_expense_count ec ON ro.id = ec.org_id
LEFT JOIN policy_summary ps ON ro.id = ps.org_id
LEFT JOIN active_users au2 ON ro.id = au2.org_id
LEFT JOIN total_expense_amount te ON ro.id = te.org_id
LEFT JOIN total_users_who_expensed tu ON ro.id = tu.org_id
LEFT JOIN most_common_expense_type mc ON ro.id = mc.org_id
LEFT JOIN total_expense_types tt ON ro.id = tt.org_id;
