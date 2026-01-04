-- WorkSafe AI
-- Generate explainable insights using Snowflake Cortex

CREATE OR REPLACE VIEW AI_RISK_SUMMARY AS
SELECT
  INDUSTRY,
  DEPARTMENT,
  STAFF_ID,
  RISK_FLAG,
  SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    CONCAT(
      'Explain in simple, non-technical language why this work-hour pattern may be risky. ',
      'Industry: ', INDUSTRY, '. ',
      'Risk type: ', RISK_FLAG, '. ',
      'Focus on fatigue, wellbeing, and safety. ',
      'Do not blame individuals.'
    )
  ) AS AI_EXPLANATION
FROM UNSAFE_PATTERNS;
