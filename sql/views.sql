-- Average length of stay by encounter type  
CREATE OR REPLACE VIEW public.vw_avg_los_by_encounter_type AS
SELECT
    encounter_type,
    AVG(length_of_stay_days) AS avg_length_of_stay_days,
    COUNT(*) AS encounters
FROM public.fact_encounter
GROUP BY encounter_type
ORDER BY encounter_type;


-- Monthly encounters and charges by department`
CREATE OR REPLACE VIEW public.vw_encounters_by_department_month AS
SELECT
    department_id,
    DATE_TRUNC('month', admit_date) AS month,
    COUNT(*) AS encounter_count,
    SUM(total_charges) AS total_charges
FROM public.fact_encounter
GROUP BY department_id, DATE_TRUNC('month', admit_date)
ORDER BY month, department_id;
