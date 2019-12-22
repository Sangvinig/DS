WITH days AS
  ( SELECT cdate::TIMESTAMP
   FROM entity.calendar
   WHERE 1=1
     AND cdate <= CURRENT_DATE
     AND cdate >= dateadd('year', -3, CURRENT_DATE)
   ORDER BY cdate ASC)
   
SELECT 
    cdate,
    manager_id,
    manager_role,
    COUNT (DISTINCT company_id)
FROM days
LEFT JOIN datasource.corp_responsible_manager resp
    ON days.cdate BETWEEN resp.start_date AND resp.end_date
GROUP BY cdate, manager_id, manager_role
